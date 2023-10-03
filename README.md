# Spotify to Universal Scrobbler

Convert Spotify history JSON files to CSV for universalscrobbler.com

https://github.com/Sulius/Spotify-to-LastFM-Converter didn't work for me, so I wrote my own version using just the Python standard libraries.

## Usage

```sh
python convert.py <input_file.json>
```

This will produce a number of CSV files of 2800 lines each (the most that can be scrobbled in one day) in the correct format for
The Universal Scrobbler's bulk importer: https://universalscrobbler.com/bulk.php

The script will filter out any plays shorter than 30s and will remove any duplicates plays that may appear.

Tested with Python 3.8 but should work with other versions.

## TODO

- Make the output file size configurable
- Run on a whole directory of JSON files as Spotify splits its exports by year.
