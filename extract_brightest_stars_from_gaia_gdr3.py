"""
////////////////////////////////////////////////////////////////////////////////////////////////////
//                         This file is part of the CosmoScout VR ecosystem                       //
////////////////////////////////////////////////////////////////////////////////////////////////////

SPDX-FileCopyrightText: German Aerospace Center (DLR) <cosmoscout@dlr.de>
SPDX-FileCopyrightText: Bauhaus-Universität Weimar / Adrian Kreskowski <adrian.kreskowski@uni-weimar.de>
SPDX-License-Identifier: MIT

This script downloads, unpacks and processes the entire Gaia GDR3 star catalogue one data chunk at
a time and compiles a CSV file containing a target number of the brightest stars with respect to 
highest G-band mean magnitude.

If desired, the script performs cross-matching with the Hipparcos 

"""

import csv  # for reading the CSV chunks line by line
import gzip  # for decoding the GZipped CSV chunks
import shutil  # for shell-style copy operations
from bs4 import BeautifulSoup  # for parsing the HTML in combination with webrequests
from decimal import Decimal  # for turning our string-based sorting key into a decimal number
import requests  # for performing webrequests in the first place
import os  # for creation of intermediate folders and listdir


"""
    #############################################################
    #                       DATATYPE DEFINITIONS                #
    #############################################################
"""

"""
    Gaia star datatype containing the attributes relevant for the current
    version of Cosmoscout-VR (https://github.com/cosmoscout/cosmoscout-vr).
    The script disregards stars which do not contain valid attributes for
    our five attributes 'ra', 'dec', 'parallax', 'bp_rp' and 'bp_g'
"""


class GaiaStar:
    def __init__(self, source_id, hipparcos_id, ra, dec, parallax, phot_g_mean_mag, bp_rp):
        self.source_id = source_id
        self.hipparcos_id = hipparcos_id
        self.ra = ra
        self.dec = dec
        self.parallax = parallax
        self.phot_g_mean_mag_as_decimal = Decimal(phot_g_mean_mag)
        self.phot_g_mean_mag = phot_g_mean_mag
        self.bp_rp = bp_rp

    def __lt__(self, other):
        return self.phot_g_mean_mag_as_decimal < other.phot_g_mean_mag_as_decimal


"""
    #############################################################
    #                   FUNCTION DEFINITIONS                    #
    #############################################################
"""

# Parse html-page, taken from:
# https://gist.github.com/icarrr/91a9b289c20a841b93f1567516c7046e


def listFD(url, ext=''):
    page = requests.get(url).text
    soup = BeautifulSoup(page, 'html.parser')
    return [url + '/' + node.get('href') for node in soup.find_all('a')
            if node.get('href').endswith(ext)]


def find_csv_filenames(path_to_dir, suffix=".csv"):
    filenames = listdir(path_to_dir)
    return [(path_to_dir + filename) for filename in filenames if filename.endswith(suffix)]


"""
    #############################################################
    #                       CONFIGURABLES                       #
    #############################################################
"""

"""
'target num_stars_in_list' is the desired number of the brightest stars which
will be contained in the compiled CSV file.
"""
target_num_stars_in_list = 5_000_000  # 5m star as default value

"""
'keywords_and_indices' is a dictionary which will be filled by parsing the
chunk header to avoid hardcoding the actual column positions of our desired
attributes. This allows for quicker adaptation of the script.
"""
keywords_and_indices = {"source_id": -1, "ra": -1, "dec": -1, "parallax": -1, "phot_g_mean_mag": -1, "bp_rp": -1}

"""
for testing purposes one can parse a subset of the first couple of gaia chunks.
setting 'stop_after_x_num_chunks' to a number <= 0 will parse all chunks
"""
stop_after_x_num_chunks = 0


"""
    #############################################################
    #                       NON-CONFIGURABLES                   #
    #############################################################
"""

"""
    path to the gaia gdr3 dataset
"""
url = 'https://cdn.gea.esac.esa.int/Gaia/gdr3/gaia_source'

""
end_of_header_line_skip_index = 1000


"""
    'reservoir_size' is set to twice the target number of stars to avoid 
    perpetual resorting the star list every time one adds a new star
"""
reservoir_size = target_num_stars_in_list * 2

"""
    List containing our current set of the brightest stars
"""
gaia_star_list = []

num_parsed_gaia_chunks = 0
num_stars_parsed = 0

# https://itnext.io/overwrite-previously-printed-lines-4218a9563527
LINE_CLEAR = '\x1b[2K'  # <-- ANSI sequence


"""
    we are oly interested in the gzipped files since they contain gaia chunks
"""
compressed_gaia_chunk_ext = 'gz'

"""
    #############################################################
    #                       OUTPUT_DIRECTORIES                  #
    #############################################################
"""

# temporary directory for the currently downloaded chunk
compressed_gaia_directory = "csv_gz/"
# temporary directory for the currently downloaded chunk
decompressed_gaia_directory = "csv/"

# persistent output directory for the parsed stars
brightest_stars_output_directory = "output_brightest_stars/"


if not os.path.exists(compressed_gaia_directory):
    os.makedirs(compressed_gaia_directory)

if not os.path.exists(decompressed_gaia_directory):
    os.makedirs(decompressed_gaia_directory)

if not os.path.exists(brightest_stars_output_directory):
    os.makedirs(brightest_stars_output_directory)

"""
parse nearest neighbor map gaia to hipparcos for crossmatching.
We assume that for cross matching the downloaded and unpacked file
'Hipparcos2BestNeighbour.csv' from 
https://cdn.gea.esac.esa.int/Gaia/gedr3/cross_match/hipparcos2_best_neighbour/Hipparcos2BestNeighbour.csv.gz
exists next to this script. If it is not available, no crossmatching
will be performed.
"""
gaia_to_hipparcos_dict = {}
has_parsed_gaia_to_hipparcos_dict = False

try:
    with open("Hipparcos2BestNeighbour.csv") as gaia:
        csv_reader_object = csv.reader(gaia, delimiter=",")

        row_counter = 0
        for row in csv_reader_object:

            row_counter += 1

            if row_counter <= 1:
                continue
            else:
                pass

            gaia_to_hipparcos_dict[row[0]] = row[1]

        has_parsed_gaia_to_hipparcos_dict = True
except:
    print("\n")
    print("Did not find Gaia to Hipparcos crossmatching CSV file")
    pass


gaia_chunk_compressed_filename = compressed_gaia_directory + "current_compressed_gaia_chunk.csv.gz"
gaia_chunk_decompressed_filename = decompressed_gaia_directory + "current_gaia_chunk.csv"

for file in listFD(url, compressed_gaia_chunk_ext):
    if stop_after_x_num_chunks > 0:
        if num_parsed_gaia_chunks >= stop_after_x_num_chunks:
            break

    num_parsed_gaia_chunks += 1

    while True:
        try:
            print(LINE_CLEAR + "Fetching file '" + file + "' and temporarily saving it as '" +
                  gaia_chunk_compressed_filename + "'", end='\r')
            r = requests.get(file, allow_redirects=True)
            open(gaia_chunk_compressed_filename, 'wb').write(r.content)

            print(LINE_CLEAR+"Unpacking current chunk...", end='\r')
            with gzip.open(gaia_chunk_compressed_filename, 'rb') as gaia_chunk_compressed_file_in:
                with open(gaia_chunk_decompressed_filename, 'wb') as gaia_chunk_decompressed_file_out:
                    shutil.copyfileobj(gaia_chunk_compressed_file_in, gaia_chunk_decompressed_file_out)

            print(LINE_CLEAR+"Done unpacking chunk " + str(num_parsed_gaia_chunks), end='\r')
            print(LINE_CLEAR+"Parsing current chunk ", end='\r')
            with open(gaia_chunk_decompressed_filename) as gaia_chunk_decompressed_file_out:

                has_parsed_gaia_header = False
                csv_reader_object = csv.reader(gaia_chunk_decompressed_file_out, delimiter=",")

                row_counter = 0

                for row in csv_reader_object:

                    row_counter += 1
                    # skip the first thousand lines because that contains comments only
                    if row_counter <= end_of_header_line_skip_index:
                        continue
                    else:
                        pass

                    # we parse the gaia header from every file for keeping the line count intact
                    if not has_parsed_gaia_header:
                        keyword_index = 0
                        for keyword_in_row in row:
                            for keyword_in_dict in keywords_and_indices:
                                if keyword_in_dict == keyword_in_row:
                                    keywords_and_indices[keyword_in_dict] = keyword_index
                                    break

                            keyword_index += 1
                        has_parsed_gaia_header = True
                    else:
                        star_is_valid = True
                        for keyword_in_dict in keywords_and_indices:
                            if ("null" == row[keywords_and_indices[keyword_in_dict]]):
                                star_is_valid = False
                                break

                        if star_is_valid:
                            looked_up_hiparcos_id = "-1"
                            current_source_id = row[keywords_and_indices["source_id"]]
                            if has_parsed_gaia_to_hipparcos_dict:

                                if (current_source_id in gaia_to_hipparcos_dict):
                                    looked_up_hiparcos_id = gaia_to_hipparcos_dict[current_source_id]

                            gaia_star_list.append(
                                GaiaStar(
                                    row[keywords_and_indices["source_id"]],
                                    looked_up_hiparcos_id, row[keywords_and_indices["ra"]],
                                    row[keywords_and_indices["dec"]],
                                    row[keywords_and_indices["parallax"]],
                                    row[keywords_and_indices["phot_g_mean_mag"]],
                                    row[keywords_and_indices["bp_rp"]]))

                            # sort our stars if we hit our reservoir size limit
                            if (reservoir_size < len(gaia_star_list)):
                                gaia_star_list.sort()
                                gaia_star_list = gaia_star_list[0:target_num_stars_in_list]

                        if (0 == (num_stars_parsed % 10000)):
                            print(LINE_CLEAR + "Parsed: " + str(num_stars_parsed) +
                                  " stars of the gaia gdr3 dataset already", end='\r')

                        num_stars_parsed += 1

            print(LINE_CLEAR+"Done parsing chunk " + str(num_parsed_gaia_chunks), end='\r')

            # remove temporary chunk files
            if os.path.isfile(gaia_chunk_compressed_filename):
                os.remove(gaia_chunk_compressed_filename)
            if os.path.isfile(gaia_chunk_decompressed_filename):
                os.remove(gaia_chunk_decompressed_filename)
        except Exception as e:
            # if there was a problem while downloading or unpacking, try it again
            print("Could not decompress file: " + gaia_chunk_compressed_filename)
            print("I will attempt to redownload and decompress the file once again.")
            continue
        break


gaia_star_output_file = brightest_stars_output_directory + "gaia_brightest_stars__phot_g_mean_mag__bp_rp.csv"
print(LINE_CLEAR+"Done parsing " + str(stop_after_x_num_chunks) + " gaia gdr3 chunks.", end='\r')

print(LINE_CLEAR+"Performing final sorting step", end='\r')
# sort the stars one last time
gaia_star_list.sort()
# reduce stars to our target number
gaia_star_list = gaia_star_list[0:target_num_stars_in_list]

print(
    "Writing out the " + str(target_num_stars_in_list) + " brightest stars parsed from " + str(num_parsed_gaia_chunks) +
    " gaia gdr3 chunks to " + gaia_star_output_file)
# write out our brightest star CSV file
with open(gaia_star_output_file, 'wb') as f:
    csv_header_to_write = "source_id" + "|" + "hipparcos_id" + "|" + "ra" + "|" + \
        "dec" + "|" + "parallax" + "|" + "phot_g_mean_mag" + "|" + "bp_rp" + "\n"
    ascii_encoded_csv_header_to_write = csv_header_to_write.encode('ascii')
    f.write(ascii_encoded_csv_header_to_write)

    for star_idx in range(0, len(gaia_star_list)):
        current_star_to_write = gaia_star_list[(len(gaia_star_list) - 1) - star_idx]
        star_string_to_write = current_star_to_write.source_id + "|" + current_star_to_write.hipparcos_id + "|" + current_star_to_write.ra + "|" + \
            current_star_to_write.dec + "|" + current_star_to_write.parallax + "|" + current_star_to_write.phot_g_mean_mag + "|" + current_star_to_write.bp_rp + "\n"
        ascii_encoded_star_string_to_write = star_string_to_write.encode('ascii')

        f.write(ascii_encoded_star_string_to_write)


# remove the temporary folders *.csv and *.csv_gz
try:
    shutil.rmtree(compressed_gaia_directory)
    shutil.rmtree(decompressed_gaia_directory)
except OSError as e:
    print("Error removing directory: %s - %s." % (e.filename, e.strerror))
