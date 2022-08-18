#![allow(clippy::let_underscore_drop)]
#![allow(clippy::cast_possible_truncation)]

use itertools::Itertools;
use std::collections::HashMap;
use std::env;
use std::error::Error;
use std::io;
use std::path;

#[derive(serde::Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
enum TxType {
    Deposit,
    Withdrawal,
    Dispute,
    Resolve,
    Chargeback,
}
#[derive(PartialEq)]
enum DepositState {
    NotApplicable,
    Deposited,
    InDispute,
}

impl Default for DepositState {
    fn default() -> Self {
        Self::NotApplicable
    }
}
#[derive(serde::Deserialize)]
struct InputRecord {
    #[serde(rename(deserialize = "type"))]
    tx_type: TxType,
    #[serde(skip_deserializing)]
    deposit_state: DepositState,
    #[serde(rename(deserialize = "client"))]
    client_id: u16,
    #[serde(rename(deserialize = "tx"))]
    tx_id: u32,
    amount: Option<f32>,
}

#[derive(Copy, Clone)]
struct OutputRecord {
    available: i64,
    held: i64,
    total: i64,
    locked: bool,
}

impl OutputRecord {
    const fn new(amount: i64) -> Self {
        Self {
            available: amount,
            held: 0,
            total: amount,
            locked: false,
        }
    }
}

/// Returns true if the client account is locked, false otherwise.
fn is_client_locked(client_id: u16, client_map: &HashMap<u16, OutputRecord>) -> bool {
    if let Some(output_record) = client_map.get(&client_id) {
        if output_record.locked {
            return true;
        }
    }
    false
}

/// Process the input record.
/// # Arguments
///
/// * `record_res` - A result from the csv deserializer. If the result is an error, the record is ignored.
/// * `tx_map` - A map of transaction IDs to their associated input record. Invalid transactions are not kept.
/// * `client_map` - A map from a client ID to their associated output record. This map holds all the processed output records.
fn process_input_record(
    record_res: Result<InputRecord, csv::Error>,
    tx_map: &mut HashMap<u32, InputRecord>,
    client_map: &mut HashMap<u16, OutputRecord>,
) {
    let record = match record_res {
        Ok(record_res) => record_res,
        // If there is an error parsing the input (e.g client_id is missing), we assume it's erroneous and ignore it.
        Err(_) => return,
    };

    // handle the transaction. Just ignore transactions which fail and continue
    let _ = match &record.tx_type {
        TxType::Deposit => handle_deposit(record, tx_map, client_map),
        TxType::Withdrawal => handle_withdraw(record, tx_map, client_map),
        TxType::Dispute => handle_dispute(&record, tx_map, client_map),
        TxType::Resolve => handle_resolve(&record, tx_map, client_map),
        TxType::Chargeback => handle_chargeback(&record, tx_map, client_map),
    };
}

/// Handles deposit transactions
fn handle_deposit(
    mut record: InputRecord,
    tx_map: &mut HashMap<u32, InputRecord>,
    client_map: &mut HashMap<u16, OutputRecord>,
) -> Result<(), Box<dyn Error>> {
    // If transaction was already processed or client account is frozen, we fail the transaction.
    if tx_map.contains_key(&record.tx_id) || is_client_locked(record.client_id, client_map) {
        Err("invalid")?;
    }

    let client_id = record.client_id;

    // if the amount is missing in the input for a deposit, assume it's erroneous and fail the transaction.
    let amount = match record.amount {
        Some(amount) => {
            if amount < 0f32 {
                Err("negative")?;
            }
            (amount * 1e4).round() as i64
        }
        None => Err("missing amount")?,
    };

    record.deposit_state = DepositState::Deposited;
    // Save the record in case it's later disputed and so we don't process it more than once.
    tx_map.insert(record.tx_id, record);

    // Update the output records
    match client_map.get_mut(&client_id) {
        Some(output_record) => {
            output_record.available += amount;
            output_record.total += amount;
        }
        None => {
            let output_record = OutputRecord::new(amount);
            client_map.insert(client_id, output_record);
        }
    }
    Ok(())
}

/// Handles withdraw transactions
fn handle_withdraw(
    record: InputRecord,
    tx_map: &mut HashMap<u32, InputRecord>,
    client_map: &mut HashMap<u16, OutputRecord>,
) -> Result<(), Box<dyn Error>> {
    // If transaction was already processed or client account is frozen, we fail the transaction.
    // If the client account is frozen, we do not need to store this transaction
    if tx_map.contains_key(&record.tx_id) || is_client_locked(record.client_id, client_map) {
        Err("invalid")?;
    }

    let client_id = record.client_id;

    // if the amount is missing in the input for a withdrawal, assume it's erroneous and fail the transaction.
    let amount = match record.amount {
        Some(amount) => {
            if amount < 0f32 {
                Err("negative")?;
            }
            (amount * 1e4).round() as i64
        }
        None => Err("missing amount")?,
    };

    // Save the record so that we don't process this transaction twice in case we receive same transaction ID more than once.
    tx_map.insert(record.tx_id, record);

    // Update the output records
    match client_map.get_mut(&client_id) {
        Some(output_record) => {
            // if there is not enough funds in the account, fail the transaction.
            if amount > output_record.available {
                Err("rejected")?;
            }
            output_record.available -= amount;
            output_record.total -= amount;
        }
        // If there is no record of this client, their asset account may still be valid even if the
        // transaction should fail. So include this client account in the output with 0 funds.
        None => {
            let output_record = OutputRecord::new(0);
            client_map.insert(client_id, output_record);
            Err("rejected")?;
        }
    }
    Ok(())
}

/// Handles dispute transactions
fn handle_dispute(
    record: &InputRecord,
    tx_map: &mut HashMap<u32, InputRecord>,
    client_map: &mut HashMap<u16, OutputRecord>,
) -> Result<(), Box<dyn Error>> {
    let disputed_tx_record = match tx_map.get_mut(&record.tx_id) {
        Some(input_record) => input_record,
        // I assume that this is an erroneous transaction since it's disputing a non-existing transaction.
        None => Err("invalid")?,
    };

    // The client should not be able to dispute transactions that do not belong to their account
    // and the only valid transactions to process are deposits that are not in dispute.
    // We also reject handling disputes for accounts which are locked/frozen.
    if disputed_tx_record.client_id != record.client_id
        || disputed_tx_record.deposit_state != DepositState::Deposited
        || is_client_locked(record.client_id, client_map)
    {
        Err("rejected")?;
    }

    // If the amount is missing on the input record or the client account
    // is missing from our output records, this is an unrecoverable error.
    let amount_to_hold = (disputed_tx_record.amount.unwrap() * 1e4) as i64;
    let client_output_record = client_map.get_mut(&disputed_tx_record.client_id).unwrap();

    disputed_tx_record.deposit_state = DepositState::InDispute;

    client_output_record.available -= amount_to_hold;
    client_output_record.held += amount_to_hold;

    Ok(())
}

/// Handles resolve transactions
fn handle_resolve(
    record: &InputRecord,
    tx_map: &mut HashMap<u32, InputRecord>,
    client_map: &mut HashMap<u16, OutputRecord>,
) -> Result<(), Box<dyn Error>> {
    let disputed_tx_record = match tx_map.get_mut(&record.tx_id) {
        Some(input_record) => input_record,
        // I assume that this is an erroneous transaction since it's disputing a non-existing transaction.
        None => Err("invalid")?,
    };

    // The client should not be able to resolve transactions that do not belong to their account
    // and the only valid transactions to process are deposits that are in dispute.
    // We also reject handling disputes for accounts which are locked/frozen.
    if disputed_tx_record.client_id != record.client_id
        || disputed_tx_record.deposit_state != DepositState::InDispute
        || is_client_locked(record.client_id, client_map)
    {
        Err("rejected")?;
    }

    // If the amount is missing this is a programming error, unrecoverable error.
    let amount_to_resolve = (disputed_tx_record.amount.unwrap() * 1e4) as i64;
    // If the client account is missing this is a programming error, unrecoverable error.
    let client_output_record = client_map.get_mut(&disputed_tx_record.client_id).unwrap();

    disputed_tx_record.deposit_state = DepositState::Deposited;
    client_output_record.available += amount_to_resolve;
    client_output_record.held -= amount_to_resolve;
    Ok(())
}

/// Handles chargeback transactions
fn handle_chargeback(
    record: &InputRecord,
    tx_map: &mut HashMap<u32, InputRecord>,
    client_map: &mut HashMap<u16, OutputRecord>,
) -> Result<(), Box<dyn Error>> {
    let disputed_tx_record = match tx_map.get_mut(&record.tx_id) {
        Some(input_record) => input_record,
        // I assume that this is an erroneous transaction since it's disputing a non-existing transaction.
        None => Err("invalid")?,
    };

    // The client should not be able to issue chargebacks on transactions which do not belong to their account
    // and the only valid transactions to process are deposits that are in dispute.
    // We also reject handling disputes for accounts which are locked/frozen.
    if disputed_tx_record.client_id != record.client_id
        || disputed_tx_record.deposit_state != DepositState::InDispute
        || is_client_locked(record.client_id, client_map)
    {
        Err("rejected")?;
    }

    // If the amount is missing on the input record or the client account
    // is missing from our output records, this is an unrecoverable error.
    let amount_to_withdraw = (disputed_tx_record.amount.unwrap() * 1e4) as i64;
    let client_output_record = client_map.get_mut(&disputed_tx_record.client_id).unwrap();

    // Just update the client account and mark as frozen, the transactions' state no longer matters.
    client_output_record.held -= amount_to_withdraw;
    client_output_record.total -= amount_to_withdraw;
    client_output_record.locked = true;
    Ok(())
}

// Writes the client_map's output records to writer.
fn write_output(
    client_map: &HashMap<u16, OutputRecord>,
    writer: impl io::Write,
) -> Result<(), Box<dyn Error>> {
    #![allow(clippy::cast_precision_loss)]
    let mut wtr = csv::Writer::from_writer(writer);
    wtr.write_record(&["client", "available", "held", "total", "locked"])?;
    // There's no requirement to sort by client id but I find that it's easier to read this way.
    for client_id in client_map.keys().sorted() {
        let output_record = client_map.get(client_id).unwrap();
        wtr.write_record(&[
            format!("{}", client_id),
            format!("{:.4}", (output_record.available as f32) / 1e4),
            format!("{:.4}", (output_record.held as f32) / 1e4),
            format!("{:.4}", (output_record.total as f32) / 1e4),
            format!("{}", output_record.locked),
        ])?;
    }
    Ok(())
}

/// Process the csv file pointed to by `csv_file_path` and populate `client_map` with the output records
/// * `csv_file_path` - A path to the csv file.
/// * `client_map` - A map from a client ID to their associated output record. This map holds all the processed output records.
fn process_csv_file(csv_file_path: &path::Path, client_map: &mut HashMap<u16, OutputRecord>) {
    // This maps tx_ids to previously processed transactions. Invalid transactions are not kept.
    let mut tx_map: HashMap<u32, InputRecord> = HashMap::new();

    let mut csv_reader = match csv::ReaderBuilder::new()
        .trim(csv::Trim::All)
        .from_path(csv_file_path)
    {
        Ok(rdr) => rdr,
        Err(error) => panic!(
            "Failed to read {}: {error}",
            csv_file_path.to_str().unwrap()
        ),
    };

    for record in csv_reader.deserialize() {
        process_input_record(record, &mut tx_map, client_map);
    }
}

fn main() {
    let args: Vec<String> = env::args().collect();
    assert_eq!(args.len(),2, "Requires one and only one argument. Run like `cargo run -- transaction.csv > accounts.csv`");
    let csv_file = &args[1][..];

    // This maps the client_id to an output record which will be output at the end of the program in csv format to stdout.
    let mut client_map: HashMap<u16, OutputRecord> = HashMap::new();

    process_csv_file(path::Path::new(csv_file), &mut client_map);

    if let Err(err) = write_output(&client_map, io::stdout()) {
        eprintln!("Error writing to stdout: {}", err);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // convenience method to validate that internal i64 representation matches expected float value.
    fn assert_amount(amount: i64, num: f32) {
        assert_eq!((num * 1e4) as i64, amount);
    }

    // Test the output for a basic withdraw/deposit cases with different amounts
    // Client 2 will decline a withdrawal because they are short 0.0001
    // Client 1 will receive a duplicate deposit (tx 1), it will be ignored
    // Client 3 will deposit and withdraw to the smallest decimal precision
    // Client 4 will deposit 1 billion dollars and then reject a withdrawal / deposit for negative amounts
    #[test]
    fn basic_test() {
        let basic_csv_file = path::Path::new("sample_data/deposit_withdraw.csv");
        let mut client_map: HashMap<u16, OutputRecord> = HashMap::new();
        process_csv_file(basic_csv_file, &mut client_map);

        let mut writer = io::BufWriter::new(Vec::new());

        write_output(&client_map, &mut writer).unwrap();

        let bytes = writer.into_inner().unwrap();

        let mut rdr = csv::Reader::from_reader(io::BufReader::new(&*bytes));
        for result in rdr.records() {
            let record: csv::StringRecord = result.unwrap();
            let client_id = record.get(0).unwrap();
            let available = record.get(1).unwrap();
            let held = record.get(2).unwrap();
            let total = record.get(3).unwrap();
            // client 1
            if client_id == "1" {
                assert_eq!(available, "0.0001");
                assert_eq!(total, "0.0001");
                assert_eq!(held, "0.0000");
            }
            // client 2
            if client_id == "2" {
                assert!(available == "2.0000");
                assert!(total == "2.0000");
                assert_eq!(held, "0.0000");
            }
            // client 3
            if client_id == "3" {
                assert_eq!(available, "0.0000");
                assert_eq!(total, "0.0000");
                assert_eq!(held, "0.0000");
            }
            // client 4
            if client_id == "4" {
                assert_eq!(available, "1000000000.0000");
                assert_eq!(total, "1000000000.0000");
                assert_eq!(held, "0.0000");
            }
        }
    }

    // Tests dispute/resolve/chargeback logic.
    #[test]
    fn disputes_test() {
        let disputes_csv_file = path::Path::new("sample_data/disputes.csv");
        let mut client_map: HashMap<u16, OutputRecord> = HashMap::new();
        let mut tx_map: HashMap<u32, InputRecord> = HashMap::new();

        let mut csv_reader = match csv::ReaderBuilder::new()
            .trim(csv::Trim::All)
            .from_path(disputes_csv_file)
        {
            Ok(rdr) => rdr,
            Err(error) => panic!(
                "Failed to read {}: {error}",
                disputes_csv_file.to_str().unwrap()
            ),
        };

        let mut iter = csv_reader.deserialize();
        // process the first two deposits
        process_input_record(iter.next().unwrap(), &mut tx_map, &mut client_map);
        process_input_record(iter.next().unwrap(), &mut tx_map, &mut client_map);

        // Process the first dispute
        process_input_record(iter.next().unwrap(), &mut tx_map, &mut client_map);
        {
            let client1_record = client_map.get(&1).unwrap();

            assert_amount(client1_record.held, 500_f32);
            assert_amount(client1_record.available, 0_f32);
            assert_amount(client1_record.total, 500_f32);
            assert!(!client1_record.locked);

            let tx_1 = tx_map.get(&1).unwrap();
            assert!(tx_1.deposit_state == DepositState::InDispute);
        }

        // Process the second dispute. client 1 cannot dispute client 2 transaction -> ignored.
        process_input_record(iter.next().unwrap(), &mut tx_map, &mut client_map);
        {
            let client2_record = client_map.get(&2).unwrap();
            assert_amount(client2_record.held, 0_f32);
            assert_amount(client2_record.available, 5_f32);
            assert_amount(client2_record.total, 5_f32);
            assert!(!client2_record.locked);

            let tx_2 = tx_map.get(&2).unwrap();
            assert!(tx_2.deposit_state == DepositState::Deposited);
        }

        // Process the resolution of first dispute.
        process_input_record(iter.next().unwrap(), &mut tx_map, &mut client_map);
        {
            let client1_record = client_map.get(&1).unwrap();
            assert_amount(client1_record.held, 0_f32);
            assert_amount(client1_record.available, 500_f32);
            assert_amount(client1_record.total, 500_f32);
            assert!(!client1_record.locked);

            let tx_1 = tx_map.get(&1).unwrap();
            assert!(tx_1.deposit_state == DepositState::Deposited);
        }

        // Process second dispute for tx 1
        process_input_record(iter.next().unwrap(), &mut tx_map, &mut client_map);
        {
            let client1_record = client_map.get(&1).unwrap();
            assert_amount(client1_record.held, 500_f32);
            assert_amount(client1_record.available, 0_f32);
            assert_amount(client1_record.total, 500_f32);
            assert!(!client1_record.locked);

            let tx_1 = tx_map.get(&1).unwrap();
            assert!(tx_1.deposit_state == DepositState::InDispute);
        }

        // Process another deposit while in dispute for client 1
        process_input_record(iter.next().unwrap(), &mut tx_map, &mut client_map);
        {
            let client1_record = client_map.get(&1).unwrap();
            assert_amount(client1_record.held, 500_f32);
            assert_amount(client1_record.available, 5_f32);
            assert_amount(client1_record.total, 505_f32);
            assert!(!client1_record.locked);
        }

        // Process tx 1 chargeback
        process_input_record(iter.next().unwrap(), &mut tx_map, &mut client_map);
        {
            let client1_record = client_map.get(&1).unwrap();
            assert_amount(client1_record.held, 0_f32);
            assert_amount(client1_record.available, 5_f32);
            assert_amount(client1_record.total, 5_f32);
            assert!(client1_record.locked);
        }

        // Process client 1 trying to deposit more funds. Rejected.
        process_input_record(iter.next().unwrap(), &mut tx_map, &mut client_map);
        {
            let client1_record = client_map.get(&1).unwrap();
            assert_amount(client1_record.held, 0_f32);
            assert_amount(client1_record.available, 5_f32);
            assert_amount(client1_record.total, 5_f32);
            assert!(client1_record.locked);
        }

        // Process client 1 trying to withdraw funds. Rejected.
        process_input_record(iter.next().unwrap(), &mut tx_map, &mut client_map);
        {
            let client1_record = client_map.get(&1).unwrap();
            assert_amount(client1_record.held, 0_f32);
            assert_amount(client1_record.available, 5_f32);
            assert_amount(client1_record.total, 5_f32);
            assert!(client1_record.locked);
        }
    }
}
