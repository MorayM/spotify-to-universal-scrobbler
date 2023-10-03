import sys
import csv
import json

DELIM = ","
UPLOAD_LIMIT = 2800

def chunk(l, n):
	for i in range(0, len(l), n):
		yield l[i:i + n]

def main():
	input_file_name = sys.argv[1]
	output_file_name = sys.argv[2] if len(sys.argv) == 3 else "output.csv"

	input_file = open(input_file_name, "r", encoding="UTF8")
	all_data = json.load(input_file)
	input_file.close()

	all_rows = []
	rows_set = set() # for de-duplication

	for play in all_data:
		duration_ms = play["ms_played"]
		artist = play["master_metadata_album_artist_name"]
		title = play["master_metadata_track_name"]
		if not artist or not title or duration_ms < 30000:
			continue
		row = [artist, title, play["master_metadata_album_album_name"], play["ts"], "", str(int(duration_ms / 1000))]
		print(row)
		stringified_row = DELIM.join(row)
		if stringified_row not in rows_set:
			all_rows.append(row)
			rows_set.add(stringified_row)

	file_chunks = list(chunk(all_rows, UPLOAD_LIMIT)) # chunk() returns a generator, not a list

	for i in range(0, len(file_chunks)):
		with open(f"output-{i}.csv", "w") as output_file:
			writer = csv.writer(output_file)
			writer.writerows(file_chunks[i])

main()

