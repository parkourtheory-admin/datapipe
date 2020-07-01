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