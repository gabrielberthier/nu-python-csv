# A python library to parse and collect data from nubank invoices/bank statements (in csv format)

## How to use

Simply pass a folder or file to `parse_csv` function and an output where you want to store your results.
This library separates your invoices or bank statements from Nubank in expenses and incomes to improve readability.

```py
await parse_csv('./myfolder', 'default_output')

```
