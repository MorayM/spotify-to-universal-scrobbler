import sys
import os
import glob
import csv
import json
import argparse
from datetime import datetime

parser = argparse.ArgumentParser(description="Process Spotify JSON exports for loading into Universal Scrobbler")
parser.add_argument("-n", "--num", help="Number of rows to include in each output file", default=2400, type=int)
parser.add_argument("-o", "--output", help="Directory to place output files", default=".")
parser.add_argument("-f", "--from", help="Only take plays recorded after this date (ISO format)", default=None, dest="from_date")
parser.add_argument("-t", "--to", help="Only take plays recorded before this date (ISO format)", default=None, dest="to_date")
parser.add_argument("input", help="Path to directory with input files to process")

DELIM = ","
OUTPUT_FILE_NAME = "output"

def chunk(l, n):
	for i in range(0, len(l), n):
		yield l[i:i + n]

def is_naive(d):
	return d.tzinfo is None or d.tzinfo.utcoffset(d) is None

def ensure_tz(d):
	if is_naive(d):
		return d.astimezone()
	else:
		return d

def main():
	args = parser.parse_args()
	output_path = os.path.abspath(args.output)
	date_from = ensure_tz(datetime.fromisoformat(args.from_date)) if args.from_date else None
	date_to = ensure_tz(datetime.fromisoformat(args.to_date)) if args.to_date else None

	if not os.path.isdir(args.input):
		print("Provided path is not a directory")
		exit()

	if not os.path.exists(output_path):
		os.makedirs(output_path)

	input_files = glob.glob(os.path.join(os.path.abspath(args.input), "*.json"))

	all_input_data = []
	for file in input_files:
		with open(file, "r", encoding="UTF8") as f:
			all_input_data += json.load(f)

	all_rows = []
	rows_set = set() # for de-duplication

	for play in all_input_data:
		date_played = ensure_tz(datetime.fromisoformat(play["ts"].replace('Z', '+00:00')))
		duration_ms = play["ms_played"]
		artist = play["master_metadata_album_artist_name"]
		title = play["master_metadata_track_name"]

		# Last.fm needs both an artist and title, and will only count plays over 30s
		if not artist or not title or duration_ms < 30000:
			continue
		# Filter out plays by date
		if (date_from and date_played >= date_from) or (date_to and date_played <= date_to):
			continue

		row = [artist, title, play["master_metadata_album_album_name"], play["ts"], "", str(int(duration_ms / 1000))]
		stringified_row = DELIM.join(row)
		if stringified_row not in rows_set:
			all_rows.append(row)
			rows_set.add(stringified_row)

	file_chunks = list(chunk(all_rows, args.num)) # chunk() returns a generator, not a list

	for i in range(0, len(file_chunks)):
		output_file = os.path.join(output_path, f"{OUTPUT_FILE_NAME}-{i}.csv")
		with open(output_file, "w") as output_file:
			writer = csv.writer(output_file)
			writer.writerows(file_chunks[i])

main()

