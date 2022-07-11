# Downloads a Spotify playlist into a folder of MP3 tracks
# Jason Chen, 21 June 2020

import os
import spotipy
import spotipy.oauth2 as oauth2
import yt_dlp
from youtube_search import YoutubeSearch
import multiprocessing
import configparser
import pandas as pd
import csv

# **************PLEASE READ THE README.md FOR USE INSTRUCTIONS**************

def write_tracks(text_file: str, tracks: dict, playlist_name: str):
    # Writes the information of all tracks in the playlist to a text file. 
    # This includins the name, artist, and spotify URL. Each is delimited by a comma.
    with open(text_file, 'w+', encoding='utf-8') as file_out:
        while True:
            for item in tracks['items']:
                if 'track' in item:
                    track = item['track']
                else:
                    track = item
                try:
                    track_url = track['external_urls']['spotify']
                    track_name = track['name']
                    track_artist = track['artists'][0]['name']
                    csv_line = track_name + "," + track_artist + "," + playlist_name + "," + track_url + "\n"
                    try:
                        file_out.write(csv_line)
                    except UnicodeEncodeError:  # Most likely caused by non-English song names
                        print("Track named {} failed due to an encoding error. This is \
                            most likely due to this song having a non-English name.".format(track_name))
                except KeyError:
                    print(u'Skipping track {0} by {1} (local only?)'.format(
                            track['name'], track['artists'][0]['name']))
            # 1 page = 50 results, check if there are more pages
            if tracks['next']:
                tracks = spotify.next(tracks)
            else:
                break

def write_playlist(username: str, playlist_id: str):
    results = spotify.user_playlist(username, playlist_id, fields='tracks,next,name')
    playlist_name = results['name']
    text_file = u'{0}.txt'.format(playlist_name, ok='-_()[]{}')
    print(u'Writing {0} tracks to {1}.'.format(results['tracks']['total'], text_file))
    tracks = results['tracks']
    write_tracks(text_file, tracks, playlist_name)
    return playlist_name

def find_and_download_songs(reference_file: str):
    if not os.path.exists('tracks_downloaded.csv'):
        with open("tracks_downloaded.csv", 'a', encoding='utf-8') as f:
            csv.writer(f).writerow(['Title','Artist','Playlist','Url'])
    df = pd.read_csv('tracks_downloaded.csv')

    TOTAL_ATTEMPTS = 10
    with open(reference_file, "r", encoding='utf-8') as file:
        for line in file:
            csv_line = line.split(",")
            if csv_line[3].replace('\n','') in list(df['Url']):
                continue
            with open("tracks_downloaded.csv", 'a', encoding='utf-8') as f:
                csv.writer(f, quoting=csv.QUOTE_NONE, escapechar=' ', lineterminator='').writerow(csv_line)
            name, artist = csv_line[0], csv_line[1]
            text_to_search = artist + " - " + name
            best_url = None
            attempts_left = TOTAL_ATTEMPTS
            while attempts_left > 0:
                try:
                    results_list = YoutubeSearch(text_to_search, max_results=1).to_dict()
                    best_url = "https://www.youtube.com{}".format(results_list[0]['url_suffix'])
                    break
                except IndexError:
                    attempts_left -= 1
                    print("No valid URLs found for {}, trying again ({} attempts left).".format(
                        text_to_search, attempts_left))
            if best_url is None:
                print("No valid URLs found for {}, skipping track.".format(text_to_search))
                continue
            # Run you-get to fetch and download the link's audio
            print("Initiating download for {}.".format(text_to_search))
            ydl_opts = {
                'format': 'bestaudio/best',
                'postprocessors': [{
                    'key': 'FFmpegExtractAudio',
                    'preferredcodec': 'mp3',
                    'preferredquality': '320',
                }],
            }
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                ydl.download([best_url])

# Multiprocessed implementation of find_and_download_songs
# This method is responsible for manging and distributing the multi-core workload
def multicore_find_and_download_songs(reference_file: str, cpu_count: int):
    # Extract songs from the reference file
    
    lines = []
    with open(reference_file, "r", encoding='utf-8') as file:
        for line in file:
            lines.append(line)

    # Process allocation of songs per cpu
    number_of_songs = len(lines)
    songs_per_cpu = number_of_songs // cpu_count

    # Calculates number of songs that dont evenly fit into the cpu list
    # i.e. 4 cores and 5 songs, one core will have to process 1 extra song
    extra_songs = number_of_songs - (cpu_count * songs_per_cpu)

    # Create a list of number of songs which by index allocates it to a cpu
    # 4 core cpu and 5 songs [2, 1, 1, 1] where each item is the number of songs
    #                   Core 0^ 1^ 2^ 3^
    cpu_count_list = []
    for cpu in range(cpu_count):
        songs = songs_per_cpu
        if cpu < extra_songs:
            songs = songs + 1
        cpu_count_list.append(songs)

    # Based on the cpu song allocation list split up the reference file
    index = 0
    file_segments = []
    for cpu in cpu_count_list:
        right = cpu + index
        segment = lines[index:right]
        index = index + cpu
        file_segments.append(segment)
    
    # Prepares all of the seperate processes before starting them
    # Pass each process a new shorter list of songs vs 1 process being handed all of the songs
    processes = []
    segment_index = 0
    for segment in file_segments:
        p = multiprocessing.Process(target = multicore_handler, args=(segment, segment_index,))
        processes.append(p)
        segment_index = segment_index + 1

    # Start the processes
    for p in processes:
        p.start()

    # Wait for the processes to complete and exit as a group
    for p in processes:
        p.join()

# Just a wrapper around the original find_and_download_songs method to ensure future compatibility
# Preserves the same functionality just allows for several shorter lists to be used and cleaned up
def multicore_handler(reference_list: list, segment_index: int):
    # Create reference filename based off of the process id (segment_index)
    reference_filename = "{}.txt".format(segment_index)
    
    # Write the reference_list to a new "reference_file" to enable compatibility
    with open(reference_filename, 'w+', encoding='utf-8') as file_out:
        for line in reference_list:
            file_out.write(line)

    # Call the original find_and_download method
    find_and_download_songs(reference_filename)    

    # Clean up the extra list that was generated
    if(os.path.exists(reference_filename)):
        os.remove(reference_filename)


# This is prompt to handle the multicore queries
# An effort has been made to create an easily automated interface
# Autoeneable: bool allows for no prompts and defaults to max core usage
# Maxcores: int allows for automation of set number of cores to be used
# Buffercores: int allows for an allocation of unused cores (default 1)
def enable_multicore(autoenable=False, maxcores=None, buffercores=1):
    native_cpu_count = multiprocessing.cpu_count() - buffercores
    if autoenable:
        if maxcores:
            if(maxcores <= native_cpu_count):
                return maxcores
            else:
                print("Too many cores requested, single core operation fallback")
                return 1
        return multiprocessing.cpu_count() - 1
    multicore_query = "Y"
    if multicore_query not in ["Y","y","Yes","YES","YEs",'yes']:
        return 1
    core_count_query = 4
    if(core_count_query == 0):
        return native_cpu_count
    if(core_count_query <= native_cpu_count):
        return core_count_query
    else:
        print("Too many cores requested, single core operation fallback")
        return 1

def load_playlists():
    return (
        [
            config['PLAYLIST']['PURE_TRIP'],
            config['PLAYLIST']['HOUSE'],
            config['PLAYLIST']['BIG_ROOM'],
            config['PLAYLIST']['UNKNOWN_TECH'],
            config['PLAYLIST']['MY_FAVORITE'],
            config['PLAYLIST']['TECH_HOUSE'],
            config['PLAYLIST']['PROG_TECH'],
            config['PLAYLIST']['BUSHER_GUILLANO'],
            config['PLAYLIST']['PROG_HOUSE'],
            config['PLAYLIST']['ABOVE_BEYOND'],
            config['PLAYLIST']['SOIREE_2020'],
            config['PLAYLIST']['MOOD'],
            config['PLAYLIST']['CLASSIC_TECH_ACID'],
            config['PLAYLIST']['HARDWELL'],
            config['PLAYLIST']['HARD_TRANCE'],
            config['PLAYLIST']['ORJAN_NILSEN'],
            config['PLAYLIST']['IAM'],
            config['PLAYLIST']['BENNY_BENASSI'],
            config['PLAYLIST']['ARMIN_VAN_BUUREN'],
            config['PLAYLIST']['BICEP'],
            config['PLAYLIST']['THE_THRILLSEEKERS'],
            config['PLAYLIST']['FESTIVAL'],
            config['PLAYLIST']['KEY4050'],
            config['PLAYLIST']['PSYTRANCE'],
            config['PLAYLIST']['TINLICKER'],
            config['PLAYLIST']['UPLIFTING'],
            config['PLAYLIST']['SUPER8_TAB'],
            config['PLAYLIST']['LINKIN_PARK'],
            config['PLAYLIST']['DUBVISION'],
            config['PLAYLIST']['ROBBIE_WILLIAMS'],
            config['PLAYLIST']['SECRET_SET'],
            config['PLAYLIST']['NORA_EN_PURE'],
            config['PLAYLIST']['CAMELPHAT'],
            config['PLAYLIST']['SULTAN_SHEPARD'],
            config['PLAYLIST']['ALY_FILA'],
            config['PLAYLIST']['ADRIATIQUE'],
            config['PLAYLIST']['PIANO'],
            config['PLAYLIST']['AKON'],
            config['PLAYLIST']['SOLARSTONE'],
            config['PLAYLIST']['GIUSEPPE_OTTIVIANI'],
            config['PLAYLIST']['ATYPIC_PLAYLIST'],
            config['PLAYLIST']['VARIETE_FR'],
            config['PLAYLIST']['TUBE_AMBIANCE'],
            config['PLAYLIST']['TUPAC'],
            config['PLAYLIST']['DEPECHE_MODE'],
            config['PLAYLIST']['CAR_MUSIC'],
            config['PLAYLIST']['LYRICS_TRANCE'],
            config['PLAYLIST']['ATYPIC_PLAYLIST'],
            config['PLAYLIST']['EVANESCENCE'],
            config['PLAYLIST']['PUSH'],
            config['PLAYLIST']['FISHER'],
            config['PLAYLIST']['DEADMAU5'],
            config['PLAYLIST']['JOEYSUKI'],
            config['PLAYLIST']['SOME_THINGS'],
            config['PLAYLIST']['VICETONE'],
            config['PLAYLIST']['DASH_BERLIN'],
            config['PLAYLIST']['OLD_TIESTO'],
            config['PLAYLIST']['COSMIC_GATE'],
            config['PLAYLIST']['FERRY_CORSTEN'],
            config['PLAYLIST']['ANDREW_RAYEL'],
            config['PLAYLIST']['DAVID_GUETTO'],
            config['PLAYLIST']['SKI'],
            config['PLAYLIST']['PARTY_1'],
            config['PLAYLIST']['PARTY_2'],
            config['PLAYLIST']['PARTY_3'],
            config['PLAYLIST']['PARTY_4'],
            config['PLAYLIST']['PARTY_5'],
            config['PLAYLIST']['PARTY_6'],
            config['PLAYLIST']['PARTY_7'],
            config['PLAYLIST']['PARTY_8']
        ]
    )

if __name__ == "__main__":
    # Parameters
    print("Please read README.md for use instructions.")
    config = configparser.ConfigParser()
    config.read('config.ini')
    client_id = config['CREDENTIAL']['client_id']
    client_secret = config['CREDENTIAL']['client_secret']
    username = config['CREDENTIAL']['username']

    playlist_list = load_playlists()

    multicore_support = enable_multicore(autoenable=False, maxcores=None, buffercores=1)
    auth_manager = oauth2.SpotifyClientCredentials(client_id=client_id, client_secret=client_secret)
    spotify = spotipy.Spotify(auth_manager=auth_manager)
    for playlist_uri in playlist_list:
        playlist_name = write_playlist(username, playlist_uri)
        reference_file = "{}.txt".format(playlist_name)
        # Create the playlist folder
        if not os.path.exists(playlist_name):
            os.makedirs(playlist_name)
        os.rename(reference_file, playlist_name + "/" + reference_file)
        os.chdir(playlist_name)
        # Enable multicore support
        if multicore_support > 1:
            multicore_find_and_download_songs(reference_file, multicore_support)
        else:
            find_and_download_songs(reference_file)
        os.chdir("..")
    print("Operation complete.")
