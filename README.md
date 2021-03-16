# datapipe
Parkour Theory data set processing pipeline

Before running `main.py`, set up your configuration file ending in `.ini` and place it in the `configs/` directory. Also update `whitelist.ini` with row ids as needed.

### Requirements
1. pystagram
```
git clone https://github.com/ch3njust1n/pystagram.git && cd pystagram
python setup.py build
sudo python setup.py install
```

### Basic configuration setup
Change the move and video configurations to match your directory structure.

If `dataset` parameters are set, then `BagOfWordsMultihot` and `BagOfWordsOnehot` will output separate files for each split. Else, they will output a single json
of the entire data set without splits.

```
[DEFAULT]
whitelist = yes
parallel  = no
pipe      = FixEmbed, CheckMoves

[moves]
csv  = data/database/latest/moves.tsv

[videos]
src      = data/videos/production
dst      = data/videos/test
csv      = data/database/latest/videos.tsv
csv_out  = data/output
height   = 150
width    = 150

[thumbnails]
height = 300
width  = 168
dst    = data/thumbnails

[dataset]
train_split = .8
val_split   = .1
test_split  = .1
```

### Usage
```
--config  -cfg 	Configuration file (available: production, test)
--clean   -c    Clean out old logs
--verbose -v    Display stack trace if errors occur
```

### Example usage

To run the `test.ini` configuration:
```
python main.py -cfg test
```

### Available pipeline tasks
```
CollectVideos		Find missing videos, collect videos with sources, and update and save video table.
DuplicateEdges		Find duplicate edges for manual correction.
ExtractThumbnails 	Extract thumbnails from videos as base64 strings, and update and save video table.
FixEmbed 			Assume mp4 file names were named correctly, and update and save embed strings.
FixExtensions 		Rename video files so they end in .mp4.
FormatVideos 		Parallelize processing of resizing video frames and save videos.
Incomplete 			Find all empty rows in move table for manual correction.
InvalidIDs 			Find moves with incorrect ids for manual correction.
MoveTypes 			Create list of canonical move types and log errors for manual correction.
PruneGraph 			Prune the knowledge graph of incomplete entities without video information.
SortEdges			Sort move edges and save to tsv.
Symmetry			Check symmetry of move edges and log for manual correction.
```

### Notes
1. Running `FixEmbed` task will output a tsv, which if opened in Google sheets or Libre Office 5 will show malformed cells containing entire rows. Not sure why this happens, but the same tsv file can be viewed properly with pandas in a Jupter Notebook or imported into MySQL. Querying those same malformed rows in either pandas or MySQL will results in correctly formatted data.
2. Run ExtrapolationMasks, PruneGraphMasks, and RandomMasks separate from BagOfWordsMultihot, and BagOfWordsOnehot. When running BagOfWordsMultihot, and BagOfWordsOnehot, need to change config output directory to that task.