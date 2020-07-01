# datapipe
Parkour Theory data set processing pipeline

Before running `datapipe.py`, set up your configuration file ending in `.ini` and place it in the `configs/` directory. Also update `whitelist.ini` as needed. Then run the script using the following flags.

### Basic configuration setup
Change the move and video configurations to match your directory structure. Otherwise to simply run the pipeline, set these three parameters:

-- config -cfg	Configuration file in config/

```
[DEFAULT]
whitelist = yes
parallel  = no
pipe      = collect_videos, check_moves
```

### Usage
```
--config -cfg 	Configuration file (available: production, test)
```

### Example usage

To run the `test.ini` configuration:
```
python datapipe.py -cfg test
```