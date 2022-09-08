# Description

This is a toy payment engine that receives a csv file with columns `type`, `client`, `tx`, and `amount`. It outputs a csv with columns:

- `available`: The total funds that are available
- `held`: The total funds that are held for dispute
- `total`: The total funds that are available or held (available + held)
- `locked`: Whether the account is locked. An account is locked if a charge back occurs

This application assumes a precision of four palces past the decimal in the output, and outputs values with that level of precision.

A transaction can be either a withdrawal or deposit. Deposits can be disputed.

A dispute represents a client's claim that a transaction was erroneous and should be reversed. The transaction shouldn't be reversed yet but the associated funds should be held. This means that the clients available funds should decrease by the amount disputed, their held funds should increase by the amount disputed, while their total funds should remain the same. Disputes reference a transaction ID.

A resolve represents a resolution to a dispute, releasing the associated held funds. Funds that were previously disputed are no longer disputed. This means that the clients held funds should decrease by the amount no longer disputed, their available funds should increase by the amount no longer disputed, and their total funds should remain the same.

A chargeback is the final state of a dispute and represents the client reversing a transaction. Funds that were held have now been withdrawn. This means that the clients held funds and total funds should decrease by the amount previously disputed. If a chargeback occurs the client's account should be immediately frozen/locked.

# How to run

```
cargo run -- transaction.csv > accounts.csv
```

# Memory Requirements

Since there is a requirement that this is a _simple_ rust crate, I'm not going to use a database. In fact, I'm going to assume that if you run this with a very large amount of transactions that you will have the memory for it. So how much memory might this engine require?

We need to reference earlier transactions when there's a dispute, resolve or chargeback. There are 2^32 possible transactions that this program can handle before the transaction IDs overflow. Each input record is 16 bytes:

- Transaction type enum = 1 byte
- Deposit State = 1 byte
- Client ID = 2 bytes
- Transaction ID = 4 bytes
- Option&lt;Amount&gt; = 8 bytes (storing it as an option doubles the size)

So storing all the input records alone would take (2^32 transactions)\*16 bytes ~= 68.7GB.

Then the hashmap which maps the transaction id to the input record would require additional storage (at the time of writing this, the std collection uses a [hashbrown](https://github.com/rust-lang/hashbrown) implementation with 1 byte of overhead per entry). The output records would also be kept in memory however there would be a maximum of 2^16 output transactions so it would be a relatively small amount of memory.

If this engine needed to support processing a very large amount of transactions, it would make sense to persist the input records instead of keeping them all in memory.

# Assumptions

- I am assuming that this payment engine does not need to handle ridiculously large numbers, (e.g larger than 10^14)
- When a client account is locked, the client cannot perform further transactions. Transactions to a locked client account will be ignored.
- I am assuming that only deposits can be disputed since the challenge says that when a transaction is disputed, the client's "available funds should decrease by the amount disputed, their held funds should increase by the amount disputed, while their total funds should remain the same". Reversing a withdrawal would induce the opposite of the described behavior which I am assuming would be undesirable based on this description. Therefore disputed transactions which refer to withdrawals are assumed to be erroneous and are thus ignored.
- I am assuming that if there isn't enough available funds in the account to reverse a deposit, that available/total funds should go to negative.
- If the type, client or tx fields are missing, or there are less than 4 columns in the csv record then I assume the transaction is erroneous and ignore it.
- If a withdrawal or deposit is missing the amount field or it's negative, I assume the transaction is erroneous and ignore it.
- I assume that if a dispute is resolved, it can be disputed again later.

# Further Comments

- I chose to represent the amounts in the input as 64 bit signed integers to avoid floating point operations. Since we only need 4 points of decimal precision, we can just treat each integer in the output records as an amount of 0.0001 which is the smallest amount of precision we need to handle. I could have also used BigInt if I wanted to handle large numbers, but I thought this would be unneessary for a toy payment engine. If it wasn't impossible that accounts could expect to hold more than i64::MAX / 1e4 funds in their account, then I would change my assumption.
- I wrote two test cases, one to validate the output and one to validate the internal payment processing logic. There are also csv files I used to test my code manually in sample_data/
