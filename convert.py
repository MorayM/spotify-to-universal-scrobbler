import sys
import os
import glob
import csv
import json
import argparse

parser = argparse.ArgumentParser(description="Process Spotify JSON exports for loading into Universal Scrobbler")
parser.add_argument("-n", "--num", help="Number of rows to include in each output file", default=2400, type=int)
parser.add_argument("-o", "--output", help="Directory to place output files", default=".")
parser.add_argument("input", help="Path to directory with input files to process")

DELIM = ","
OUTPUT_FILE_NAME = "output"

def chunk(l, n):
	for i in range(0, len(l), n):
		yield l[i:i + n]

def main():
	args = parser.parse_args()
	output_path = os.path.abspath(args.output)

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
		duration_ms = play["ms_played"]
		artist = play["master_metadata_album_artist_name"]
		title = play["master_metadata_track_name"]
		if not artist or not title or duration_ms < 30000:
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

