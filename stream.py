#!/usr/bin/env python3.4

# Stream tweets for a particular filter from the API.
# A sample of 1000 tweets with the filter "movie" collected from
# Thu Feb 19 02:50:15 +0000 2015 to Thu Feb 19 02:52:51 +0000 2015
# resulted in 4,376,897 bytes of formatted JSON or 4377 bytes/tweet
# in 156 seconds, a rate of 2424 MB/day and 553,846 tweets/day for
# an estimated 255 GB and 58,153,830 tweets in the data set.

from moviedata import DATES_MOVIES, PUNCT, nyzone # also STOPLIST
from movie import track_join
from myauth import get_my_api
import datetime
import errno
import http.client
import json
import os
import os.path
import signal
import sys
import time
import twitter
import urllib

COUNT = 10000
INDENT = 1                              # INDENT=0 doesn't help.  None?

api = get_my_api()

signal_names = [name for name in dir(signal)
                if name.startswith("SIG") and not name.startswith("SIG_")]
signal_dict = {}
for name in signal_names:
    signal_dict[getattr(signal,name)] = name

def handle_signal(signum, frame):
    """Handle signal by raising OSError."""
    raise OSError(signum, signal_dict[signum], "<OS signal>")

# We can't really use this?
# errno_names = [name for name in dir(errno) if name.startswith("E")]
# errno_dict = {}
# for name in errno_names:
#     errno_dict[getattr(errno,name)] = name

class HangupException(Exception):
    pass

i = 0
vol = 0
working = True
need_connection = True
delay = None                            # delay == 0 means disconnect occured,
                                        # but reconnect immediately.

def collect_MOVIES(DATES_MOVIES):
    MOVIES = []
    now = datetime.datetime.now(tz=nyzone)
    for date_movies in DATES_MOVIES:
        date = date_movies[0]
        if (date + datetime.timedelta(70) >= now
            and date - datetime.timedelta(7) <= now):
            MOVIES.extend(date_movies[1:])
    return MOVIES

def next_friday():
    now = datetime.datetime.now(tz=nyzone)
    offset = (4 - now.weekday() - 1) % 7 + 1
    return now.replace(hour=9, minute=0, second=0, microsecond=0) + datetime.timedelta(offset)    

signal.signal(signal.SIGHUP, handle_signal)
signal.signal(signal.SIGTERM, handle_signal)
def generate_tweets(api, movies):
    # #### Can we catch signals and properly close the stream here?
    stream = twitter.TwitterStream(auth=api.auth)
    tweets = stream.statuses.filter(track=movies, stall_warnings=True)
    for tweet in tweets:
        yield tweet

friday = datetime.datetime(1970,1,1,tzinfo=nyzone)

while working:
    iold = i
    try:
        if need_connection or datetime.datetime.now(tz=nyzone) > friday:

            friday = next_friday()
            movies = track_join(collect_MOVIES(DATES_MOVIES))

            print("Restarting connection before opening v{0:d}".format(vol))
            print("Next Friday is", friday)
            print(movies)
            sys.stdout.flush()
            if delay:
                time.sleep(delay)
            tweets = generate_tweets(api, movies)

        # #### results/20150325.091639/stream-results-13.json was left open
        # and stream.py restarted.  It appears to have skipped over that
        # file?  (It's empty in the next series, too.)  What happened here?
        with open("stream-results-%d.json" % vol, "w") as f:
            # #### When things break, we see
            # 1. stream-results-###.json in the directory list
            # 2. it does not show up as open in lsof
            # 3. a TCP connection to Twitter does not show up in lsof

            # #### We could get and save one tweet here, and then reset
            # delay and need_connection if successful.

            for tweet in tweets:
                print(json.dumps(tweet, indent=INDENT), file=f)
                if tweet.get('hangup'):
                    raise HangupException()
                i = i + 1
                # #### Do we actually need this flush?
                if i % 100 == 0:
                    f.flush()
                # normal file rotation
                if i % COUNT == 0:
                    break
            need_connection = False

    # Handle signals, exiting somewhat gracefully by default.
    # SIGHUP breaks out of iteration and starts new volume.
    #
    # Twitter sez:
    # Reconnecting
    #
    # Once an established connection drops, attempt to reconnect
    # immediately. If the reconnect fails, slow down your reconnect
    # attempts according to the type of error experienced.
    #
    # Connection churn
    #
    # Repeatedly opening and closing a connection (churn) wastes
    # server resources. Keep your connections as stable and long-lived
    # as possible.
    #
    # (Some stuff moved to where it applies.  Irrelevant (?) stuff elided.)

    # #### This analysis is very confused and may be incomplete.  FIXME!
    # The errors and exceptions below signal that something bad happened.
    # In most cases the connection broke but we can continue.  Now:
    # If we can't continue, we don't need a connection.
    # The common case is normal loop termination and a new file.
    # So:
    # If working is set to False, we don't need to specify need_connection.
    # Since need_connection is False normally, each of the below should specify
    # working = False or need_connection = True (but not both).

    except HangupException as e:
        print(e)
        need_connection = True
    except OSError as e:
        print(e)
        if e.errno == signal.SIGTERM:
            print("%s caught signal %d%s\n%s." \
              % (time.ctime(), e.errno, ", exiting",
                 e.strerror if hasattr(e, 'strerror') else "<no strerror>"))
            print(i, "tweets done.")
            sys.stdout.flush()
            sys.exit(0)
        if e.errno not in (signal.SIGHUP, errno.ENOTRECOVERABLE, errno.EIO):
            if delay is None:
                delay = 15
            elif delay < 600:
                delay = delay*2
        need_connection = True
        print("%s caught signal %d%s\n%s." \
              % (time.ctime(), e.errno or 0, ", exiting" if not working else "",
                 e.strerror if hasattr(e, 'strerror') else "<no strerror>"))
    except StopIteration as e:          # Shouldn't happen (should be caught
                                        # by HangupException), but if it does,
        print(e)                        # AFAIK it's safe to continue

                                            # errors actually observed:
    except twitter.api.TwitterError as e:   # TwitterHTTPError
        print(type(e))
        print(e)
        # AFAIK most of these errors indicate we should stop.
        #
        # 406 stops.  Normally it's a program error:
        #   Twitter sent status 406 for URL:
        #   1.1/statuses/filter.json using parameters: (...)
        #   details: b'Parameter track item index 69 too long: \
        #   The Longest Ride Clouds o\r\n'
        #
        # Other HTTP statuses:
        # https://dev.twitter.com/streaming/overview/connecting
        # https://dev.twitter.com/overview/api/response-codes
        if str(e).startswith("Twitter sent status 503"):
            need_connection = True
            if delay is None:
                delay = 0
            elif delay >= 5:
                delay = delay*2
            else:
                delay = 5
        elif str(e).startswith("Twitter sent status 420"):
            need_connection = True
            # Example:
            #   Twitter sent status 420 for URL:
            #   1.1/statuses/filter.json using parameters: (...)
            #   details: b'Easy there, Turbo. Too many requests recently. \
            #   Enhance your calm.\r\n'
            # Twitter sez:
            #   Back off exponentially for HTTP 420 errors. Start with a
            #   1 minute wait and double each attempt. Note that every
            #   HTTP 420 received increases the time you must wait until
            #   rate limiting will no longer will be in effect for your
            #   account.
            # I'd like to add "and delay < 15*60" ... .
            if delay is not None and delay >= 60:
                delay = 2*delay
            elif delay is None or delay < 60:
                delay = 60
        elif str(e).startswith("Twitter sent status 406"):
            # This is usually a broken filter expression.
            working = False
        else:
            working = False             # Be cautious, don't know what it is!
    except (urllib.error.HTTPError,
            urllib.error.URLError,      # Temporary failure in name resolution
            http.client.HTTPException,  # BadStatusLine (empty)
            ) as e:
        print(type(e))
        print(e)
        need_connection = True
        # I believe these are continuable.
        # Twitter sez:
        #   Back off exponentially for HTTP errors for which
        #   reconnecting would be appropriate. Start with a 5 second
        #   wait, doubling each attempt, up to 320 seconds.
        if delay is None:
            delay = 0
        elif delay < 5:
            delay = 5
        elif delay >= 5 and delay < 320:
            delay = 2*delay
        else:                           # delay >= 320
            pass
    except ConnectionError as e:        # ConnectionResetError
        print(type(e))
        print(e)
        need_connection = True
        # Twitter sez:
        #   Back off linearly for TCP/IP level network errors. These
        #   problems are generally temporary and tend to clear
        #   quickly. Increase the delay in reconnects by 250ms each
        #   attempt, up to 16 seconds.
        if delay is None or delay < 16:
            delay = delay + 0.250       # If there was an HTTP error, it
                                        # won't kill us to wait a few secs.
                                        # We could introduce a flag, but....
        else:
            delay = 2*delay             # We are in the process of backing
                                        # off another error, and did not yet
                                        # get a connection.  We should not
                                        # short-circuit that backoff.
    except BaseException as e:          # #### Is catch-all the TRT?
        print(type(e))
        print(e)
        need_connection = True

    if i <= iold + 5:                   # We processed wa-a-ay too few tweets!
        # This gets up to a minute in at most 8 consecutive errors.
        # Then we should generate at most 1448 files per day in worst case.
        if delay is None:
            delay = 5
        elif delay and delay < 60:
            delay = 2*delay
        else:
            pass
        print("Setting delay to {0} due to short loop".format(delay))
    else:
        delay = None
    sys.stdout.flush()
    vol = vol + 1

print(i, "tweets done.")
sys.stdout.flush()

