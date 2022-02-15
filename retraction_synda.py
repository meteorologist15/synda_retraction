#!/usr/bin/env python3

import subprocess
import requests
import json
import os
import re
import argparse
import logging
import csv
import shutil

retracted_exps = "retracted_exps.csv" #Generic info list from file here.
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

handle_api = "https://hdl.handle.net/api/handles/"


def get_tracking_ids(pid):

    # All pid's begin with 'hdl:'. Start at index 4 for api
    pid = pid[4:]
    web_output = requests.get(handle_api + pid)
    json_output = web_output.json()
    tracking_ids = json_output['values'][4]['data']['value'] # A semicolon-separated string is returned
    return tracking_ids


def set_synda_path():

    synda_home = "/path/to/synda/sdt"
    synda_execdir = synda_home + "/bin:"
    
    if not os.getenv("ST_HOME", default=0):
        os.environ["ST_HOME"] = synda_home

    if not synda_execdir in os.getenv("PATH"):
        os.environ["PATH"] = synda_execdir + os.getenv("PATH")


def write_pid_json(retraction_list):

    pid_file = "pid_retractions.csv"
    success_count = 0
    failure_count = 0
    with open(pid_file, 'ab') as f:
        for line in retraction_list:

            # Dump metadata for each dataset using Synda to retrieve the PID
            meta_cmd = ["synda", "dump", line.rstrip("\n")]
            meta_dump = subprocess.run(meta_cmd, stdout=subprocess.PIPE)
            meta_json = meta_dump.stdout.decode("utf-8")

            if len(meta_json) > 0:
                meta_json = json.loads(meta_json)

                try:
                    pid_output = meta_json[0]["pid"]
                except (IndexError, KeyError) as err:
                    logging.error("Problems reading JSON. Skipping.\n%s\n" % err)
                    failure_count += 1
                    continue

                tracking_ids = get_tracking_ids(pid_output)

                # Write the dataset PID, dataset ID, and semicolon separated tracking_id's to file

                final_output_line = "%s,%s,%s\n" % (pid_output, line.replace("\n", ""), tracking_ids)
                f.write(bytes(final_output_line, "UTF-8"))
                success_count += 1

                logging.info("%s/%s dataset pid's written" % (str(success_count), str(len(retraction_list))))
            else:
                logging.warning("The dataset %s was not processed. Did not find metadata dump from Synda. Skipping." % line)


class Retractor(object):

    def __init__(self, institutions=None, project=None):
    
        self.project = "CMIP6" if project is None else ','.join(project)


    def clean_list(self, synda_search):

        # Some preprocess cleaning here #

        for i in range(len(synda_search)):
            synda_search[i] = re.search('[A-Z].*', synda_search[i]).group() + "\n"

        return synda_search
 

    def finalize_list(self, search_command):

        search_proc = subprocess.run(search_command, stdout=subprocess.PIPE)
        search_output = search_proc.stdout.decode("utf-8").splitlines()
        retract_list = self.clean_list(search_output) # Without the extra text in the beginning
        return retract_list



        def get_processed_retractions(self):

        # This is if you have run the script before. pid_retractions.csv is an already completed set of retractions, i.e. already "processed". Read this into memory for eventual comparison with new retraction list.

        pid_retracted_file = "pid_retractions.csv"
        already_processed = []
        if os.path.exists(pid_retracted_file):
            with open(pid_retracted_file, 'r') as f:
                already_processed = f.readlines()

        return already_processed



    def refine_list(self, retraction_list):

        # Compare with those already processed from beforehand/previous as retracted. This cancels out some redundancy.
        processed_list = self.get_processed_retractions()
        for line in processed_list:
            comparison = re.search(',(.*),', line).group(1) + "\n"
            if comparison in retraction_list:
                try:
                    retraction_list.remove(comparison)
                except ValueError as err:
                    logging.warning("Something went terribly wrong. Skipping.")
                    continue

        return retraction_list


    def get_retraction_list(self):

        main_list = []
        memory_list = []

        # Read generic retraction info into memory
        try:
            with open(retracted_exps, newline='') as f:
                esgf_reader = csv.reader(f, quotechar='|', quoting=csv.QUOTE_MINIMAL)
                for row in esgf_reader:
                    memory_list.append(row)

        except OSError as e:
            logging.error("The file %s doesn't exist. Quitting." % retracted_exps)
            raise


        # Use Synda to search by experiment_id, refine the list, and add continually add retractions to main list

        for sublist in memory_list:
            search_cmd = ["synda", "search", "project=CMIP6", "retracted=true", "experiment_id=%s" % sublist[0], "-l", "8999"]
            if sublist[2] == 'no_overflow':
                logging.info("Getting data from experiment %s" % sublist[0])
                refined_list = self.refine_list(self.finalize_list(search_cmd))
                if len(refined_list) > 0:
                    main_list.extend(refined_list)
                    logging.info("Added %s entries from this experiment. Total to add: %s" % (str(len(refined_list)), str(len(main_list))))
                else:
                    logging.info("No entries to add for this experiment")

            # Execute searches using variant label instead of simply experiment_id if retractions >= 9000

            else:
                logging.info("Getting data from experiment %s. Over 9000 retractions." % sublist[0])
                ensemble_dict = json.loads(sublist[2].replace('|', ','))
                for variant_label in ensemble_dict.keys():
                    logging.info("Getting data from variant label (ensemble) %s" % variant_label)
                    search_cmd = ["synda", "search", "project=CMIP6", "retracted=true", "experiment_id=%s" % sublist[0], "variant_label=%s" % variant_label, "-l", "8999"]
                    refined_list = self.refine_list(self.finalize_list(search_cmd))
                    if len(refined_list) > 0:
                        main_list.extend(refined_list)
                        logging.info("Added %s entries from this variant_label. Total to add: %s" % (str(len(refined_list)), str(len(main_list))))
                    else:
                        logging.info("No entries to add for this variant_label")

            
        return main_list # These are the retractions we care to add



if __name__ == "__main__":

    set_synda_path()

    logging.info("Creating PID backup...")
    shutil.copy2("pid_retractions.csv", "pid_retractions.csv.prev")
    ret = Retractor()
    myretractlist = ret.get_retraction_list()
    write_pid_json(myretractlist)
        
