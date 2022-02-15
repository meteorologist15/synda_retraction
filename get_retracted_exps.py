#!/usr/bin/env python3

import subprocess
import json
import sys
import shutil

#Color codes for easily identifying success and failure
successcolor = '\033[92m'
successcolorend = '\033[0m'
failcolor = '\033[91m'
failcolorend = '\033[0m'

csv_file = "retracted_exps.csv"

#Both Tier 1 index nodes that are considered for discovery
dkrz_site = "esgf-data.dkrz.de"
llnl_site = "esgf-node.llnl.gov"

#Construct JSON retrieval URL 
url_head = "https://" + dkrz_site + "/esg-search/search/?offset=0&limit=10&query="
url_query = "retracted%3Atrue&type=Dataset&replica=false&mip_era=CMIP6&activity_id%21=input4MIPs"
URL_TAIL = "&facets=mip_era%2Cactivity_id%2Cmodel_cohort%2Cproduct%2Csource_id%2Cinstitution_id%2Csource_type%2Cnominal_resolution%2Cexperiment_id%2Csub_experiment_id%2Cvariant_label%2Cgrid_label%2Ctable_id%2Cfrequency%2Crealm%2Cvariable_id%2Ccf_standard_name%2Cdata_node&format=application%2Fsolr%2Bjson"
url_string = url_head + url_query + URL_TAIL


#Create a backup of previous iteration of general retraction information
print("Creating backup...")
shutil.copy2(csv_file, csv_file + ".prev")


### RUN WGET COMMAND TO RETRIEVE JSON ###

print("Retrieving JSON from ESGF...")
try:
    proc = subprocess.run(["wget", url_string, "-O", "tmp.json"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True)
    print("\n%sWget command succeeded for DKRZ!%s\n" % (successcolor, successcolorend))

#if DKRZ doesn't work or is down for maintenance, try the LLNL node (will add other nodes in the future)
except subprocess.CalledProcessError as e:
    print("\n%sWget command failed for DKRZ!%s\n" % (failcolor, failcolorend))
    print("Trying LLNL...")
    url_head = "https://" + llnl_site + "/esg-search/search/?offset=0&limit=10&query="
    url_string = url_head + url_query + URL_TAIL

    try:
        proc = subprocess.run(["wget", url_string, "-O", "tmp.json"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True)
        print("\n%sWget command succeeded for LLNL!%s\n" % (successcolor, successcolorend))
    except subprocess.CalledProcessError as e:
        print("\n%sWget command failed for LLNL!%s\n" % (failcolor, failcolorend))
        print("Giving up!")
        raise


### EXTRACT EXP's AND RETRACTION COUNTS FROM JSON ###

with open("tmp.json", "r") as f:
    json_data = json.load(f)

exp_list = json_data["facet_counts"]["facet_fields"]["experiment_id"]
exp_dict = dict(zip(exp_list[::2], exp_list[1::2]))


#If greater than 9000 transactions, process separately
print("Writing file...")
with open(csv_file, "w") as g:
    for exp, num_retracted in exp_dict.items():
        if num_retracted >= 9000:
            url_query += "&experiment_id=" + exp
            url_string = url_head + url_query + URL_TAIL
            try:
                subprocess.run(["wget", url_string, "-O", "tmp2.json"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True)
            except subprocess.CalledProcessError as e:
                print("Wget command failed")
                print(e)
            
            with open("tmp2.json", "r") as h:
                json_data = json.load(h)

            ensemble_list = json_data["facet_counts"]["facet_fields"]["variant_label"]
            ensemble_dict = str(dict(zip(ensemble_list[::2], ensemble_list[1::2]))).replace(',', '|').replace("'", '"')
            g.write(exp + "," + str(num_retracted) + "," + str(ensemble_dict) + "\n")
            subprocess.run(["rm", "-f", "tmp2.json"])
        else:    
            g.write(exp + "," + str(num_retracted) + "," + "no_overflow" + "\n")


#JSON files are not necessary after the generic information is written, so you can delete the tmp files now
subprocess.run(["rm", "-f", "tmp.json"])
print("Done!")

