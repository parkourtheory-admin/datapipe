# datapipe
Parkour Theory data set processing pipeline

Before running `main.py`, set up your configuration file ending in `.ini` and place it in the `configs/` directory. Also update `whitelist.ini` with row ids as needed.

### Basic configuration setup
Change the move and video configurations to match your directory structure.

```
[DEFAULT]
whitelist = yes
parallel  = no
pipe      = FixEmbed, CheckMoves
```

### Usage
```
--config -cfg 	Configuration file (available: production, test)
```

### Example usage

To run the `test.ini` configuration:
```
python main.py -cfg test
```

### Notes
1. Running `FixEmbed` task will output a tsv, which if opened in Google sheets or Libre Office 5 will show malformed cells containing entire rows. Not sure why this happens, but the same tsv file can be viewed properly with pandas in a Jupter Notebook or imported into MySQL. Querying those same malformed rows in either pandas or MySQL will results in correctly formatted data.