import vlc
import json
import os
from datetime import datetime, timedelta
import time
import argparse


def start_record(stream_uri, outfile):
    """Record the network stream to the output file."""

    sout_cmd = "file/ts:%s" % outfile
    instance = vlc.Instance(["--sout", sout_cmd, "--no-audio"])
    media = instance.media_new(stream_uri)
    player = instance.media_player_new()
    player.set_media(media)
    player.play()
    return instance, player, media


def stop_record(instance, player, media):
    """Stop the player and clean up."""
    player.stop()
    player.release()
    instance.release()


def load_configuration(fn):
    """Load configuration from a json file."""
    with open(fn, 'r') as f:
        data = json.load(f)

    return data["stream_list"], data["schedule_template"], data["save_path"]


class Schedule:
    def __init__(self, start, end):
        self.start = start
        self.end = end


def create_schedule(template):
    """Generate a recording schedule from given template."""
    schedules = []
    now = datetime.now()
    start_time = datetime.strptime(template["start"], "%H:%M:%S")\
        .replace(year=now.year, month=now.month, day=now.day)
    end_time = datetime.strptime(template["end"], "%H:%M:%S")\
        .replace(year=now.year, month=now.month, day=now.day)
    duration = timedelta(minutes=template["duration"])
    break_time = timedelta(minutes=template["break"])

    if start_time < now:
        start_time = now

    while start_time < end_time:
        start = start_time
        start_time += duration
        end = start_time
        schedules.append(Schedule(start, end))
        start_time += break_time

    return schedules


def initialize_records(stream_list, schedules):
    """Generate records with schedule."""
    records = {}
    pid = -1
    for schedule in schedules:
        for stream in stream_list:
            pid += 1
            records[pid] = {
                "uri": stream["uri"],
                "name": stream["name"],
                "start": schedule.start,
                "end": schedule.end
            }

    return records


def generate_outfile(stream_name, save_path):
    """Generate a unique name for output file."""
    timestamp = datetime.now()
    ext = ".ts"

    # Get list of existing files
    d = set(x for x in os.listdir(save_path) if (x.endswith(ext)))

    # Filename template
    fn = [timestamp.strftime('%Y%m%d_%H%M%S'), stream_name.replace(' ', '_')]

    fn_str = '_'.join(fn)
    name = '%s%s' % (fn_str, ext)
    n = 0

    # While a filename matches the standard naming pattern, increment the
    # counter until we find a spare filename
    while name in d:
        name = '%s_%d%s' % (fn_str, n, ext)
        n += 1

    return os.path.join(save_path, name)


def print_log(text, log_file):
    """Print to the log and print to the screen with a datetime prefix."""
    now = datetime.now()
    text = "%s %s" % (now, text)
    print text
    with open(log_file, "a") as f:
        f.write(text + "\n")


def main(args):
    fn = args["config"]
    stream_list, schedule_template, save_path = load_configuration(fn)
    log_file = os.path.join(save_path, "stream.log")
    schedules = create_schedule(schedule_template)
    records = initialize_records(stream_list, schedules)
    handling_records = {}

    while len(records) > 0 or len(handling_records) > 0:
        now = datetime.now()
        rs = records.keys()
        for r in rs:
            data = records[r]
            if data["start"] < now < data["end"]:
                print_log("[INFO] started recording: %s." %data["name"], log_file)
                outfile = generate_outfile(data["name"], save_path)
                instance, player, media = start_record(data["uri"], outfile)
                data["instance"] = instance
                data["player"] = player
                data["media"] = media
                handling_records[r] = data
                records.pop(r)

        hrs = handling_records.keys()
        for hr in hrs:
            data = handling_records[hr]
            if now > data["end"]:
                print_log("[INFO] finished recording: %s." %data["name"], log_file)
                stop_record(data["instance"], data["player"], data["media"])
                handling_records.pop(hr)

        time.sleep(10)

    print_log("[INFO] no schedule, exited.", log_file)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("-c", "--config", type=str,
                    help="path to config file")
    main(vars(ap.parse_args()))
