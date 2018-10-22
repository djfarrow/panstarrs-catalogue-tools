"""
Download a PS1 or SDSS catalogue in chunks

AUTHOR: Daniel Farrow (MPE) 2017

"""
from __future__ import print_function, absolute_import

import subprocess as sp
import gc
import re
from os.path import join, isfile
from time import sleep
import suds
from argparse import ArgumentParser
from numpy import linspace, sqrt
from querystrings import *

class PSPSStatus(object):
    """ Store the PSPS status flag values """

    JOB_READY = 0
    JOB_STARTED = 1
    JOB_CANCELING = 2
    JOB_CANCELLED = 3
    JOB_FAILED = 4
    JOB_FINISHED = 5
    JOB_ALL = 6

def run_cmd(cmd, arg_=None, testmode=False):
    """ 
    Wrapper for subprocess Popen

    Parameters
    ----------
    cmd : str
        the command to run
    arg_ : str
       an argument you don't want to
       split by whitespace
    """

    # for tests just echo the command
    if testmode:
        cmd = "echo TESTMODE:" + cmd
 
    cmd = cmd.split()
    if arg_:
        cmd.append(arg_)
    # submit the job and deal with the output    
    p1 = sp.Popen(cmd, stdout=sp.PIPE, stderr=sp.PIPE,
                   universal_newlines=True)

    stdout, stderr = p1.communicate()

    return stdout, stderr


def run_query_cj(query_string, table_name, verbose=False, waittime=180., ntries=8, testmode=False, username="username"):
    """
    Submit a query to a CasJobs system

    Parameters
    ----------
    query_string : str
        the string of the SQL query you want
        to run
    table_name : str
        the database name of the query result (needs to
        the same name as within query_string)
    verbose : bool
        True or False
    waittime : float
        time to wait between attempts to 
        download the catalogue in seconds. 
    ntries : int
        Number of times to try and download the
        catalogue before giving up. 
    testmode : bool
        if True don't submit job, jsut run echo instead
        of submissiong script
    """

    # Check if file exists already
    fname = table_name + "_" + username + ".fit"
    print(fname)
    if isfile(fname):
        raise Exception("Output file exists! Remove!")

    # Stick the query in a file
    fquery = "query_tmp.{:s}".format(table_name)
    try:
        with open(fquery, 'w') as fp:
            fp.write(query_string)
    except IOError as e:
        print("Cannot produce query files. Do you have write permission in working dir?")
        raise(e)

    cmd = "java -jar casjobs.jar run -f " + fquery 

    # Run the query
    print("Running query...")
    stdout, stderr = run_cmd(cmd, testmode=testmode)
    if stderr != "":
        print("Problem with job submission!")
        print(stdout)
        print(stderr) 
    elif verbose:
        print(stdout)

    # wait a bit then try and download the catalogue
    for ntry in range(1, ntries):

        if verbose:
            print("Download attempt {:d}".format(ntry))
        download_table(table_name, fname, verbose=verbose, testmode=testmode)

        # if output file exists remove catalogue from MyDB
        if isfile(fname):       
            print("Downloaded! Removing catalogue from MyDB")
            cmd = "java -jar casjobs.jar submit -t MyDB "
            stdout, stderr = run_cmd(cmd, arg_="drop table " + table_name, testmode=testmode)
            if stderr != "":
                print(stderr)
            elif verbose:
                print(stdout)
            return             

        sleep(waittime)

    print("Could not download catalogue {:s}".format(table_name))

def wait_for_psps_job(jobsClient, sessionID, schemaGroup, job_id, waittime=5):
     
    print("Waiting for job {:d}".format(job_id))
    wait = True
    while wait:
    
       status = jobsClient.service.getJobStatus(sessionID, schemaGroup, job_id) 
       wait = (status != PSPSStatus.JOB_CANCELLED) &  (status != PSPSStatus.JOB_FAILED) 
       wait &=  (status != PSPSStatus.JOB_FINISHED)
       sleep(waittime)
    
    if status != PSPSStatus.JOB_FINISHED:
        print("The job failed or was cancelled!")
        return False
    else:
        print("Job successful! Running output job") 
        return True


def run_query_psps(authfile, table_name, query_string, waittime=5, jobType="slow", schemaGroup = "PS1_SCHEMA", 
                   username="", schema = "PanSTARRS_3PI_PV3.1"):
    """
    Run a query on the PSPS system. Much of this based on
    http://psps.ifa.hawaii.edu/psps-trac/attachment/wiki/PSPSAccess/queryDRLClient.py

    Parameters
    ----------
    authfile : str
        file containing usename and password
    table_name : str
        the database name of the query result (needs to
        the same name as within query_string)   
    query_string : str
        a string containing the SQL query
    waittime : float
        time to wait between attempts to 
        download the catalogue in seconds. 
    """

    # URLs to the PSPS SOAP WSDLs for authentication and queries.
    authWsdlUrl = "http://panstarrs.stsci.edu/DFetch/WSDL/AuthService.wsdl.php"
    jobsWsdlUrl = "http://panstarrs.stsci.edu/DFetch/WSDL/JobsService.wsdl.php"

    # Check if file exists already
    fname = table_name + "_" + username + ".fit"
    print(fname)
    if isfile(fname):
        raise Exception("Output file exists! Remove!")

    # Stick the query in a file
    fquery = "query_tmp.{:s}".format(table_name)
    try:
        with open(fquery, 'w') as fp:
            fp.write(query_string)
    except IOError as e:
        print("Cannot produce query files. Do you have write permission in working dir?")
        raise(e)

    authClient = suds.client.Client( authWsdlUrl )
    jobsClient = suds.client.Client( jobsWsdlUrl )

    print("Authenticating...")
    # Read authentication from file
    try:
        fn = open(authfile, 'r')
        username = fn.readline().strip()
        password = fn.readline().strip()
        print("Logging in as username: {:s}".format(username))
        fn.close()
    except IOError as e:
        print("Cannot read password file")
        raise(e)
        
    sessionID =  authClient.service.login( username, password)

    # Not really secure but will have to do
    del password
    gc.collect()

    print("Log in successful session token: ", sessionID)

    print("Reading query file: ",fquery)
    query = open(fquery, 'r').read()
    task = "Executing "+jobType+" query from client python script"


    # SOAP call to execute query
    if  jobType.lower() == 'fast':
      #executeQuickJob(xs:string sessionID, xs:string schemaGroup, xs:string query, xs:string context, xs:string taskname, xs:boolean isSystem, )
      queryResults = jobsClient.service.executeQuickJob( sessionID, schemaGroup, query, schema, task, False)
    elif  jobType.lower() == 'slow':
      #submitJob(xs:string sessionID, xs:string schemaGroup, xs:string query, xs:string context, xs:string taskname, xs:int TimeEstimate, )
      job_id = jobsClient.service.submitJob( sessionID, schemaGroup, query, schema, task, 600)
      queryResults = 'Job ID: ' + str(job_id)
      pass
    else:
      raise Exception("Error: unkown job type: "+jobType+". Must be either fast or slow.")

    if jobType.lower() == 'fast':
        with open(fname, "w") as outputFile:
            outputFile.write(queryResults)
            print("Wrote results to file: {:s}".format(fname))
            outputFile.close()
    else:
        print("Submitted job with ID: {:d} waiting for it to finish".format(job_id))
        if not wait_for_psps_job(jobsClient, sessionID, schemaGroup, job_id, waittime=waittime):
            return
  
        print("Running extract job!")
        job_id = jobsClient.service.submitExtractJob(sessionID, schemaGroup, table_name, "FITS")
        if not wait_for_psps_job(jobsClient, sessionID, schemaGroup, job_id, waittime=waittime):
            return
        
        print("Downloading catalogue!")
        download_table_psps(table_name, fname)

        print("Dropping table...")
        job_id = jobsClient.service.submitJob(sessionID, schemaGroup, "drop table {:s}".format(table_name), "MYDB", "Dropping table", 10)
 
    print("Done!")



def download_table_psps(table_name, filename, verbose=False, testmode=False,
                        datastore="http://ps1images.stsci.edu/datadelivery/outgoing/casjobs/psi/fits/"):
    """ 
    Download a table from PS1 data server
 
    Parameters
    ----------
    table_name : str
        name of MyDB table
    filename : str
        expected name of output
    datastore : str
        URL of where data is output to

    """

    download_url = datastore + filename

    # Look for URL in output
    stderr, stdout = run_cmd("wget " + download_url, testmode=testmode)
  
    print(stderr)


def download_table(table_name, filename, verbose=False, testmode=False,
                   datastore="http://ps1images.stsci.edu/datadelivery/outgoing/casjobs/fits/"):
    """ 
    Download a table from PS1 data server
 
    Parameters
    ----------
    table_name : str
        name of MyDB table
    filename : str
        expected name of output
    datastore : str
        URL of where data is output to

    """

    download_url = datastore + filename

    cmd = "java -jar casjobs.jar extract -url -type fits -b " + table_name

    stdout, stderr = run_cmd(cmd, testmode=testmode)

    if verbose:
        print(stdout)
        print(stderr)

    # Look for URL in output
    if download_url in stdout:
        stdout, stderr = run_cmd("wget " + download_url, testmode=testmode)
    else:
        print("Output job failed!")
        print(stderr)
        return

    if verbose:
        print(stdout)
        print(stderr)


if __name__ == '__main__':

    parser = ArgumentParser(description="Download a catalogue from PS1 or SDSS")
    parser.add_argument('--verbose', action='store_true')
    parser.add_argument('--nchunks', default=100, type=int, help="Number of chunks to split catalogue into")
    parser.add_argument('--list-file', default="cat_list.txt", help="Output file for a list of catalogues and associated limits")
    parser.add_argument('--test', action="store_true", help="Don't run queries, just go through code")
    parser.add_argument('--psps-auth', type=str, default=None, help="Pass a file with username and password to query PSPS instead of CasJobs")
    parser.add_argument('username', type=str, help="Your username on CasJobs/PSPS")
    parser.add_argument('ra_range', nargs=2, type=float, help="Lower and upper RA limits")
    parser.add_argument('dec_range', nargs=2, type=float, help="Lower and upper dec limits")
    parser.add_argument('output_template', type=str, help="Template for output catalogues, must have integer field: e.g. cat_{:d}")
    parser.add_argument('--nskip', type=int, help="Number of chunks to skip", default=0)
    opts = parser.parse_args()
   
    # Check the catalogue name has a format statement if needed
    if opts.nchunks > 1:
        if opts.output_template == opts.output_template.format(10):
            raise Exception("File template must have format statement for catalogue number: e.g. cat_{:d}")
 
    # Set up the limits of the chunks
    n = int(sqrt(opts.nchunks)) + 1
    ra_lims = linspace(opts.ra_range[0], opts.ra_range[1], n)
    dec_lims = linspace(opts.dec_range[0], opts.dec_range[1], n)
    
    # Open file to store list of downloaded catalogues
    try:
        flist = open(opts.list_file, 'w')
    except IOError as e:
        print("Cannot open list file!")
        raise(e)
    else:
        flist.write("# raLow raHigh decLow decHigh catalogue\n")
    
    tn = 0
    # Loop over the limits downloading the catalogue in chunks
    for raLow, raHigh in zip(ra_lims[:-1], ra_lims[1:]):
        for decLow, decHigh in zip(dec_lims[:-1], dec_lims[1:]):
         
            table_name = opts.output_template.format(tn)
            flist.write("{:f} {:f} {:f} {:f} {:s}\n".format(raLow, raHigh, decLow, decHigh, table_name))
 
            query = QUERY_STRING_PS1_VIEW.format(raHigh=raHigh, raLow=raLow,
                                                 decHigh=decHigh, decLow=decLow,
                                                 table_name=table_name)
            #query = TEST_QUERY_NODB.format(table_name=table_name)
    
    
            # skip queries
            if tn < opts.nskip:
                tn += 1
                continue
    
            tn += 1

            if opts.psps_auth:
                run_query_psps(opts.psps_auth, table_name, query, username=opts.username)
            else:
                run_query_cj(query, table_name, verbose=opts.verbose, testmode=opts.test, username=opts.username)  
     
            # Output this line to the cat file
            flist.flush()
            sleep(3.0) # Brief pause between queries
    
    flist.close()

