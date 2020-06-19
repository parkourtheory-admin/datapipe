# datapipe
Parkour Theory multimodal dataset

Before running `datapipe.py`, set up your configuration file ending in `.ini` and place it in the `configs/` directory. Then run the script using the following flags.

### Usage
```
--config -cfg 	Configuration file (available: production, test)
--loop -l 		Loop execution (default: False)
--pipes -p 		Specify pipelines to execute. Required by default. (options: m: move, v: video)
```

### Test configuration file
```
[DEFAULT]
whitelist = yes

[moves]
csv  = /media/ch3njus/Seagate4TB/research/parkourtheory/data/database/latest/test_moves.csv
pipe = check_moves

[videos]
src      = /media/ch3njus/Seagate4TB/research/parkourtheory/data/videos/production
dst      = /media/ch3njus/Seagate4TB/research/parkourtheory/data/videos/test
csv      = /media/ch3njus/Seagate4TB/research/parkourtheory/data/database/latest/test_videos.csv
csv_out  = /media/ch3njus/Seagate4TB/research/parkourtheory/data/output
pipe     = collect_videos

```

### Example usage

To run the `test.ini` configuration with the `move` and `video` pipelines:
```
python datapipe.py -cfg test -p m v
```