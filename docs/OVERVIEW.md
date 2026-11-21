# What this project does, in plain words

*Written for readers who don't work with data pipelines. No technical background needed.*

## The situation

Picture an online shop that doesn't hold stock itself. Instead, it sells products supplied by
**four different vendors** (we call them A, B, C and D). Every day, each vendor sends a file listing
the products it sells, with a name, a brand, a price, how many are in stock and, sometimes, a barcode.

The shop wants to answer simple-sounding questions:

- *What products do we actually have?* Not "what lines are in the files", but how many **real,
  distinct products**.
- *Who sells the same fridge cheapest today?*
- *How did that fridge's price change over the last week?*
- *Which vendor sends us the most incomplete information?*

This project builds the machinery that turns four messy daily files into trustworthy answers to
those questions. It does this automatically, every day, and gives the same answer if run twice.

We use real product names from a public Amazon dataset (household appliances, about 100,000
products). The vendors, prices, stock levels and barcodes are made up (synthetic), so nothing
private is involved.

## Why this is harder than it sounds: the pain points

### 1. Every vendor speaks a different language
- Two vendors send spreadsheets (CSV files) and two send a different format (JSON) that can hold
  information nested inside other information.
- One vendor calls the product name `title`, another calls it `name`, and the brand might arrive
  as `brand`, `manufacturer` or `maker`.
- Prices arrive as `$1,299.00` from one vendor and `1.299,00` (the European style) from another.
  To a computer these look like completely different things.

### 2. Vendors change their files without warning
On day 2 one vendor adds a new column. On day 3 another renames a column, and a third starts sending
prices as text instead of numbers. A careless system either crashes or, worse, keeps running and
silently loads the wrong data. Our system **notices every change, writes it down, and decides
what to do**:

| Change | What happens |
|---|---|
| A new column appears | Noted and ignored |
| A known rename | Handled automatically |
| An optional column goes missing | Filled with blanks, with a warning |
| A required column goes missing (e.g. no prices at all) | That vendor's file for that day is set aside ("quarantined") and the other vendors carry on |

### 3. Bad rows hide among good ones
Some rows have no price, a price that can't be read, or are duplicates. Each bad row is moved to a
"quarantine" table **with the reason written next to it**, so nothing disappears silently. The
system also checks that every incoming row is accounted for: rows in = good + quarantined +
duplicates. If the numbers don't add up, it stops.

### 4. The same product has many names (the hardest part)
Vendor A lists *"Whirlpool W10502643 Dryer Belt"*, vendor C lists *"Dryer belt for whirlpool w10502643"*
with a typo, and vendor D shortens it to *"WHIRLPOOL Belt W10502643"*. A person sees at once that
these are the same thing. A computer doesn't.

- Barcodes would solve it, but **only about half the listings have one**.
- For the rest, the system has to compare names, brands, model numbers and prices, and decide
  "same product" or "different product".
- Getting it wrong in one direction merges two different products (so a customer sees the wrong
  price). Getting it wrong in the other leaves duplicates in the catalogue.

We chose to be **cautious**: when unsure, keep products apart. Measured against a known answer key,
**98.9% of the matches the system makes are correct**, and it finds **91% of all true matches**.

### 5. The comparison problem is enormous
There are about 217,000 listings. Comparing every listing with every other would be about **23 billion
comparisons**, far too many. The trick (called *blocking*) is to only compare listings that share
a cheap clue first, such as "same brand and the same model number appears in the name". That cuts
the work to about **400,000 comparisons**, a reduction of more than 50,000 times, while still
finding most true matches. Part of this project is *measuring* that trade-off rather than guessing.

### 6. History matters, and must never be overwritten
If you only keep today's price, you can never answer "how did this price change?". The system
keeps a **timeline for every product listing**: "$49 from 1 March to 2 March, then $52 from 2 March
until now". Each change closes the old entry and opens a new one, and nothing is ever erased.

### 7. Running it again must not break anything
Jobs crash, people re-run things by accident, and days get processed twice. The system is built so
that **running the same day again changes nothing**: no double-counted prices and no duplicate
history. It remembers what it has already done in its own log books.

### 8. You can't fix what you don't measure
Every important number is written down: how many rows came in, how many were rejected and why,
how good the matching is, and how long each step took. A separate benchmark shows which tuning
settings actually make it faster on this machine.

## What comes out at the end

| Output | In plain words |
|---|---|
| Product catalogue | One clean "golden" record per real product (about 100,000), built from the best information any vendor provided |
| Cross-reference | For every vendor listing, which real product it belongs to |
| Price history | The full price and stock timeline of every listing |
| Completeness report | How often each vendor leaves the brand, model or barcode blank |
| Price-change report | Per brand and day: how many prices went up or down, and by how much on average |
| Price-spread report | For products sold by 2+ vendors: cheapest vs most expensive price today |
| Log books | What arrived, what changed in the file layouts, what was rejected, and how good matching was |

## The technical difficulty, briefly

- **Volume:** hundreds of thousands of rows a day is too much to handle comfortably one row at a time,
  so the work is spread across all the processor cores (using a tool called Apache Spark).
- **Safety:** every write either fully happens or doesn't happen at all, so a crash halfway never
  leaves half-updated tables (using a storage format called Delta Lake).
- **Judgement calls:** matching products is not exact. It needs thresholds ("how similar is similar
  enough?"), tested against an answer key and tuned. Every such choice is written down in
  `docs/DECISIONS.md` so it can be revisited.
- **Changing inputs:** the system has to be strict enough to catch broken files and flexible enough
  not to stop every time a vendor renames a column.

## Honest limits

- **Matching:** some true matches are still missed (about 9%), and a listing with no model number can
  occasionally bridge two different products.
- **Dates:** days must arrive in order. A rejected day can't be replayed after a later day has succeeded.
- **Scope:** one currency (USD), and products that disappear from a feed aren't marked as delisted.
- **Scale:** everything runs on one laptop. The same code would run on a cluster, but that hasn't been tested.
