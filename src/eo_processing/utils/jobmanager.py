from __future__ import annotations
import os
import json
import logging
import geopandas as gpd
from threading import Thread, active_count
from openeo.util import rfc3339
import re
import requests
import datetime
from pathlib import Path
import collections
import time
from openeo.extra.job_management import (MultiBackendJobManager,_format_usage_stat, JobDatabaseInterface,
                                         ignore_connection_errors, _ColumnProperties, _start_job_default,
                                         get_job_db)
from openeo.rest import OpenEoApiError
import pandas as pd
from typing import Optional, Mapping, Union, Dict, Tuple, TYPE_CHECKING, List
import openeo
import warnings
from eo_processing.utils.helper import string_to_dict

if TYPE_CHECKING:
    from eo_processing.config.data_formats import storage_option_format

logger = logging.getLogger(__name__)

class WeedJobManager(MultiBackendJobManager):
    """
    Manages jobs for a multi-backend system with capabilities to track, cancel,
    and store metadata of jobs.

    WeedJobManager is responsible for handling job operations within a
    multi-backend job management framework. It extends capabilities to track the
    status of jobs, handles error logs, manages attempts, and retains metadata
    and error information about jobs. The manager provides infrastructural
    support for operations such as downloading results or marking jobs completed
    or failed based on predefined criteria.
    """
    # alter the standard list of
    _COLUMN_REQUIREMENTS: Mapping[str, _ColumnProperties] = {
        "id": _ColumnProperties(dtype="str"),
        "backend_name": _ColumnProperties(dtype="str"),
        "status": _ColumnProperties(dtype="str", default="not_started"),
        # TODO: use proper date/time dtype instead of legacy str for start times?
        "start_time": _ColumnProperties(dtype="str"),
        "running_start_time": _ColumnProperties(dtype="str"),
        # TODO: these columns "cpu", "memory", "duration" are not referenced explicitly from MultiBackendJobManager,
        #       but are indirectly coupled through handling of VITO-specific "usage" metadata in `_track_statuses`.
        #       Since bfd99e34 they are not really required to be present anymore, can we make that more explicit?
        "cpu": _ColumnProperties(dtype="str"),
        "memory": _ColumnProperties(dtype="str"),
        "duration": _ColumnProperties(dtype="str"),
        "attempt": _ColumnProperties(dtype="int", default=0),
        "cost": _ColumnProperties(dtype="float")
    }

    def __init__(self, poll_sleep: int = 5, root_dir: str = '.',
                 storage_options: Optional[storage_option_format] = None, max_attempts: int = 3,
                 viz: bool = False, viz_labels: bool = False, viz_edge_color: str = 'black',
                 dl_cancel_time: int = 1800) -> None:
        """
        Initializes an instance of the class with configuration options for polling, directory paths,
        visualization settings, maximum retry attempts, and download cancellation timing.

        This constructor allows configuring various parameters like the polling interval, the root
        directory where operations will be performed, whether to enable visualization along with
        customizable visualization settings such as label rendering and edge colors, as well as
        maximum retry attempts. It also configures a timeout for cancellation of downloads.

        :param poll_sleep: Time in seconds to wait between polling attempts.
        :param root_dir: Path to the root directory for operations.
        :param storage_options: Dictionary-like options for setting up storage backend.
        :param max_attempts: Maximum number of retries for a failed operation.
        :param viz: Flag indicating whether to enable visualization.
        :param viz_labels: Flag indicating whether to include labels in the visualization.
        :param viz_edge_color: Color to use for edges in visualization graphs.
        :param dl_cancel_time: Time in seconds after which a download operation is canceled.
        """
        super().__init__(poll_sleep=poll_sleep, root_dir=root_dir)
        self.storage_options = storage_options if storage_options else {}
        self.viz = viz
        self.viz_labels = viz_labels
        self.viz_edge_color = viz_edge_color
        self.max_attempts = max_attempts
        self._cancel_download_after = (datetime.timedelta(seconds=dl_cancel_time))

    def download_job_too_long(self, job: openeo.BatchJob, row: pd.Series) -> bool:
        """
        Determines if a job download has been running for too long. The function calculates
        the elapsed time since a job started running and checks whether it exceeds a threshold
        determined by self._cancel_download_after and the job's current attempt count.

        :param job: Represents the job to be evaluated.
        :param row: A dictionary containing details about the job, including
                    'running_start_time' and 'duration'.
        :return: True if the job has been running longer than the allowed
                 time; False otherwise.
        """
        job_running_start_time = rfc3339.parse_datetime(row["running_start_time"], with_timezone=True)

        elapsed = (datetime.datetime.now(tz=datetime.timezone.utc) -
                   (job_running_start_time + datetime.timedelta(seconds=int(row['duration'].split(' ')[0]))))

        if elapsed > self._cancel_download_after*row['attempt']:
            f"download of job {job.job_id} (after {elapsed}) has been labeled as failed.)"
            return True
        else: return False

    def check_finished(self, job: openeo.BatchJob) -> bool:
        """
        Check if the metadata file for a given job already exists in the filesystem,
        indicating whether the job has been completed.

        This function extracts the job's metadata and determines the file path for
        the associated metadata file. It then checks if this file path exists, which
        would suggest that the job has been processed and the metadata has been saved.

        :param job: The job object whose completion status needs to be verified. It
                    must provide a 'describe' method that returns a dictionary
                    containing job metadata, including the job's title.
        :return: A boolean value where `True` indicates that the job metadata file
                 exists, and thus the job is finished. `False` signifies that the
                 metadata file is not found, indicating that the job might not be
                 complete.
        """
        job_metadata = job.describe()
        title = os.path.splitext(job_metadata['title'])[0]
        metadata_path = self.get_job_metadata_path(job.job_id, title)
        return os.path.exists(metadata_path)

    def get_job_dir(self, job_id: str) -> Path:
        """
        It primarily returns the `_root_dir` where all job directories reside. Note: that is a WEED project
        specific decision.

        :param job_id: a string identifier for the job whose directory path needs to
                       be retrieved.
        :return: the path object representing the directory of the specified job.
        """
        return self._root_dir

    def get_error_log_path(self, job_id: str, title: str = None) -> Path:
        """
        Constructs the file path for the error log associated with a specific job.
        This path is composed by joining the directory of the job, an 'errors'
        subdirectory, and a JSON file name, which is formed by concatenating the
        provided title and job ID. If the subdirectory does not exist, it will be
        created. This method does not verify the path beyond ensuring the
        existence of the necessary directory.

        :param job_id: Identifier for the job. This should be a unique string
                       associated with the job for which the error log path is
                       being generated.
        :param title: Optional title for the error log, which will be part of the
                      file name. If not provided, the file name will not have a
                      title prefix.
        :return: A Path object representing the complete file path to the error
                 log.
        """
        path  = self.get_job_dir(job_id) / "errors" / f"{title}_{job_id}_errors.json"
        if not path.parent.exists():
            path.parent.mkdir()
        return path

    def get_job_metadata_path(self, job_id: str, title: str = None) -> Path:
        """
        Constructs and returns the file path for the job's metadata based on the
        given job identifier and an optional title. The method ensures that the
        parent directory of the path exists by creating it if necessary.

        :param job_id: Unique identifier for the job, used to construct the
                       metadata file path.
        :param title: Optional descriptor to include in the metadata file name,
                      providing additional context or organization.
        :return: A Path object representing the full path to the job's metadata
                 file, which includes a JSON file named with the job_id and
                 optionally the title.
        """
        path = self.get_job_dir(job_id) / "metadata" / f"{title}_{job_id}_metadata.json"
        if not path.parent.exists():
            path.parent.mkdir()
        return path

    def get_job_graph_path(self, job_id: str, title: str  = None) -> Path:
        """
        Constructs the file path for a job graph JSON file. The path is generated
        based on the job ID and an optional title. If the directory for the
        constructed path does not exist, it is created.

        :param job_id: A unique identifier for the job.
        :param title: An optional title to include in the file name.
        :return: The path object representing the location of the job graph file.
        """
        path =  self.get_job_dir(job_id) / "jobs" / f"{title}_{job_id}_job.json"
        if not path.parent.exists():
            path.parent.mkdir()
        return path

    def on_job_error(self, job: openeo.BatchJob, row: pd.Series) -> Union[str, bool]:
        """
        Handles the logging and storage of errors encountered in a job. This method processes
        error logs from a given job, ensures the job directory exists, and writes both the
        error logs and job metadata to specified files. If no errors are encountered, a
        message indicating the absence of errors is written instead. The method finally
        returns a reason based on the error logs.

        :param job: The job instance from which error logs and metadata are retrieved.
        :param row: Row information associated with the job.
        :return: A reason derived from the error logs indicating the nature of the job's failure.
        """
        error_logs = job.logs(level="error")
        job_metadata = job.describe_job()
        title = os.path.splitext(job_metadata['title'])[0]
        error_log_path = self.get_error_log_path(job.job_id,title)
        job_graph_path = self.get_job_graph_path(job.job_id,title)

        if len(error_logs) > 0:
            self.ensure_job_dir_exists(job.job_id)
            with open(error_log_path, "w", encoding='utf8') as f:
                json.dump(error_logs, f, ensure_ascii=False, indent=2)
        else:
            error_log_path.write_text(
                "Couldn't find any errors in the logs. Please check manually.")

        # also stores the job graph of the failed job for further inspection
        with open(job_graph_path, "w", encoding='utf8') as f:
            json.dump(job_metadata, f, ensure_ascii=False, indent=2)

        return check_reason(json.dumps(error_logs, ensure_ascii=False))

    def on_job_done(self, job: openeo.BatchJob, row: pd.Series) -> None:
        """
        Handles the completion of a job by processing its metadata and results. This
        includes generating job directories, saving metadata and logs, and downloading
        the results in the correct format. Ensures the proper structure of directories
        to store job-specific data. It is responsible for adapting the download process
        based on the file format to handle special cases for NetCDF and GeoTIFF.

        :param job: The job instance that has been completed, providing access to
                    its metadata and results.
        :param row: Metadata or context information associated with the job.
        :return: None
        """
        job_metadata = job.describe()

        job_dir = self.get_job_dir(job.job_id)
        title = os.path.splitext(job_metadata['title'])[0]
        file_ext = job_metadata['process']['process_graph']['saveresult1']['arguments']['format'].lower()
        metadata_path = self.get_job_metadata_path(job.job_id,title)
        job_graph_path = self.get_job_graph_path(job.job_id,title)
        error_log_path = self.get_error_log_path(job.job_id, title)

        self.ensure_job_dir_exists(job.job_id)

        with open(job_graph_path, "w", encoding='utf8') as f:
            json.dump(job_metadata, f, ensure_ascii=False, indent=2)

        results = job.get_results()

        if self.storage_options.get('local_S3_needed', False):
            S3_prefix = self.storage_options["S3_prefix"]
            s3_objects = os.path.join(S3_prefix, title)
             
            if file_ext in ['netcdf','gtiff']:
                self.storage_options["WEED_storage"].download_s3_content(s3_objects, job_dir)

            else :
                s3_client = self.storage_options["WEED_storage"].get_s3_client()
                bucket_name= self.storage_options["WEED_storage"].s3_bucket()
                s3_client.download_file(bucket_name, os.path.join(S3_prefix,f"timeseries.{file_ext}"),
                                        job_dir / f"{title}.{file_ext}")


        if not self.storage_options.get('workspace_export', False):
            #fix prefix problem for non netcdf or GTiff files
            if file_ext in ['netcdf','gtiff']:
                results.download_files(job_dir, include_stac_metadata=False)
            else :
                results.download_file(job_dir / f"{title}.{file_ext}", name=f"timeseries.{file_ext}")


        with open(metadata_path, "w", encoding='utf8') as f:
            json.dump(results.get_metadata(), f, ensure_ascii=False, indent=2)

        logs = job.logs(level="error")

        if len(logs) > 0:
            self.ensure_job_dir_exists(job.job_id)
            with open(error_log_path, "w", encoding='utf8') as f:
                json.dump(logs, f, ensure_ascii=False, indent=2)

    def _track_statuses(self, job_db: JobDatabaseInterface, stats: Optional[Dict] = None) -> None:
        """
        Tracks and updates the statuses of jobs within the specified job database. This
        method fetches the active jobs, checks their current status, and updates it based
        on various conditions including job completion, error occurrences, and cancellation.
        It also logs the status changes, handles job retries based on specified maximum
        attempts, and persists the updated statuses back to the database. An optional
        statistics dictionary can be provided to accumulate counts of specific job
        statuses for monitoring purposes.

        :param job_db: Interface for interacting with the job database where job statuses
                       are stored and updated. It provides functionality to retrieve and
                       persist job data.
        :param stats: A dictionary object that accumulates counts and statistics of job
                      tracking operations. It can be omitted, in which case a default
                      dictionary is used to keep track of statistics within this method.
                      The dictionary includes counters for operations like job description
                      fetching, job completion, job failure, and job cancellation.
        :return: None
        """

        stats = stats if stats is not None else collections.defaultdict(int)

        active = job_db.get_by_status(statuses=["created", "queued", "running", "downloading"])

        for i in active.index:
            job_id = active.loc[i, "id"]
            backend_name = active.loc[i, "backend_name"]
            previous_status = active.loc[i, "status"]

            try:
                con = self._get_connection(backend_name)
                the_job = con.job(job_id)
                job_metadata = the_job.describe()
                stats["job describe"] += 1
                new_status = job_metadata["status"]

                logger.info(f"Status of job {job_id!r} (on backend {backend_name}) is {new_status!r} (previously {previous_status!r})")

                if previous_status in {"created", "queued"} and new_status in {"running", "finished"}:
                    stats["job started running"] += 1
                    active.loc[i, "running_start_time"] = rfc3339.utcnow()

                # get running_start_time for cases where job is finished too fast
                if new_status in {"running", "finished"}:
                    if pd.isnull(active.loc[i, "running_start_time"]) or active.loc[i, "running_start_time"]=='':
                        stats["job started running"] += 1
                        active.loc[i, "running_start_time"] = rfc3339.utcnow()

                if new_status == "finished" and previous_status != "downloading":
                    stats["job finished"] += 1
                     #Implement of max threading to avoid possible overflow of Threads
                    while active_count() > 15:
                        logger.warning(f"To many thread busy. Max 15 threads are allowed and {active_count()} are active.")
                        time.sleep(10)
                    worker = Thread(target=self.on_job_done,
                                              args=(the_job, active.loc[i]))
                    worker.start()
                    active.loc[i, "cost"] = job_metadata['costs']
                    new_status = "downloading"

                if previous_status == "downloading":
                    if not self.check_finished(the_job):
                        new_status = "downloading"

                if previous_status != "error" and new_status == "error":
                    stats["job failed"] += 1
                    error_reason = self.on_job_error(the_job, active.loc[i])
                    if error_reason:
                        new_status = error_reason
                    elif active.loc[i, "attempt"] <= self.max_attempts:
                        new_status = "not_started"
                    else:
                        new_status = "error_openeo"

                if new_status == "canceled":
                    stats["job canceled"] += 1
                    self.on_job_cancel(the_job, active.loc[i])
                    if active.loc[i, "attempt"] <= self.max_attempts:
                        new_status = "not_started"

                if self._cancel_running_job_after and new_status == "running":
                    self._cancel_prolonged_job(the_job, active.loc[i])
                # TODO: there is well hidden coupling here with "cpu", "memory" and "duration" from `_normalize_df`
                for key in job_metadata.get("usage", {}).keys():
                    if key in active.columns:
                        active.loc[i, key] = _format_usage_stat(job_metadata, key)

                #check if download is not too long and stuck (start after the first switch to downloading)
                if new_status == "downloading" and previous_status == "downloading":
                    if self.download_job_too_long(the_job, active.loc[i]):
                        # retry download
                        if active.loc[i, "attempt"] <= self.max_attempts + 2:
                            active.loc[i, "attempt"] += 1
                            new_status = "running"
                        else:
                            new_status = "error_downloading"

                active.loc[i, "status"] = new_status

            except OpenEoApiError as e:
                stats["job tracking error"] += 1
                print(f"error for job {job_id!r} on backend {backend_name}")
                print(e)

        stats["job_db persist"] += 1
        job_db.persist(active)

        if self.viz:
            self.create_viz_status(job_db)

    def _launch_job(self, start_job, df, i, backend_name, stats: Optional[Dict] = None):
        """Helper method for launching jobs

        :param start_job:
            A callback which will be invoked with the row of the dataframe for which a job should be started.
            This callable should return a :py:class:`openeo.rest.job.BatchJob` object.

            See also:
            `MultiBackendJobManager.run_jobs` for the parameters and return type of this callable

            Even though it is called here in `_launch_job` and that is where the constraints
            really come from, the public method `run_jobs` needs to document `start_job` anyway,
            so let's avoid duplication in the docstrings.

        :param df:
            DataFrame that specifies the jobs, and tracks the jobs' statuses.

        :param i:
            index of the job's row in dataframe df

        :param backend_name:
            name of the backend that will execute the job.
        """
        stats = stats if stats is not None else collections.defaultdict(int)

        df.loc[i, "backend_name"] = backend_name
        row = df.loc[i]
        try:
            logger.info(f"Starting job on backend {backend_name} for {row.to_dict()}")
            connection = self._get_connection(backend_name, resilient=True)

            stats["start_job call"] += 1
            job = start_job(
                row=row,
                connection_provider=self._get_connection,
                connection=connection,
                provider=backend_name,
            )
        except requests.exceptions.ConnectionError as e:
            logger.warning(f"Failed to start job for {row.to_dict()}", exc_info=True)
            df.loc[i, "status"] = "start_failed"
            if df.loc[i, "attempt"] <= self.max_attempts:
                df.loc[i, "attempt"] += 1
                df.loc[i, "status"] = "not_started"
            stats["start_job error"] += 1
        else:
            df.loc[i, "start_time"] = rfc3339.utcnow()
            df.loc[i, "attempt"] += 1
            if job:
                df.loc[i, "id"] = job.job_id
                with ignore_connection_errors(context="get status"):
                    status = job.status()
                    stats["job get status"] += 1
                    df.loc[i, "status"] = status
                    if status == "created":
                        # start job if not yet done by callback
                        try:
                            job.start()
                            stats["job start"] += 1
                            df.loc[i, "status"] = job.status()
                            stats["job get status"] += 1
                        except OpenEoApiError as e:
                            logger.error(e)
                            df.loc[i, "status"] = "start_failed"
                            if df.loc[i, "attempt"] <= self.max_attempts:
                                df.loc[i, "status"] = "not_started"
                            stats["job start error"] += 1
            else:
                # you can skip jobs before sending to openeo: eg some local processing needs to be finished before
                # being able to process
                df.loc[i, "status"] = "skipped"
                stats["start_job skipped"] += 1

    def create_viz_status(self, job_db: JobDatabaseInterface) -> None:
        """
        Creates a visualization of job statuses using geographic data. The status of
        each job is represented on a plot with color coding based on the current state
        of the job. Statuses and their corresponding colors are defined in a color
        dictionary. If visualization labels are enabled, each job is annotated with its
        tile ID and status on the plot.

        :param job_db: Interface to access and manipulate job data. Must implement
                       necessary methods to provide geometrical and status data as a
                       DataFrame.
        :return: None
        """
        import matplotlib.pyplot as plt
        if is_notebook():
            from IPython.display import clear_output
            clear_output(wait=True)
            plt.rcParams['figure.figsize'] = [10, 10]
            plt.rcParams['figure.dpi'] = 200

        status_df = job_db.df.copy()

        # updating the colors for processing status
        color_dict = {"not_started": 'grey',
                      "created": 'gold',
                      "queued": 'lightsteelblue',
                      "running": 'navy',
                      "start_failed": 'salmon',
                      "skipped": 'darkorange',
                      "downloading": 'lightgreen',
                      "finished": 'green',
                      "error_downloading" : "lightcoral",
                      "error": 'red',
                      "error_openeo": 'red',
                      "OOM": 'darkred',
                      "NoDataAvailable": 'darkred',
                      "orfeo_error": 'darkred',
                      "no_VH_band": 'darkred',
                      "no_tiff_in_S1": 'darkred',
                      "canceled": 'magenta'}
        status_df['color'] = status_df['status'].map(color_dict)
        status_df['color'] = status_df['color'].fillna('black')

        # plot the tiles with their status color
        fig, ax = plt.subplots()
        status_df.plot(ax=ax, edgecolor=self.viz_edge_color, color=status_df['color'], aspect='equal')

        # add labels to the tiles showing the tileID and status
        if self.viz_labels:
            status_df['coords'] = status_df['geometry'].apply(lambda x: x.representative_point().coords[:])
            status_df['coords'] = [coords[0] for coords in status_df['coords']]
            for idx, row in status_df.iterrows():
                plt.annotate(text=f"{row['name']} \n ({row['status']})", xy=row['coords'],
                             horizontalalignment='center', color='k')

        # show the figure
        plt.show()

    def start_job_thread(self, start_job, job_db):
        # Resume from existing db
        logger.info(f"Resuming `run_jobs` from existing {job_db}")
        df = job_db.read()

        self._stop_thread = False
        def run_loop():

            # TODO: support user-provided `stats`
            stats = collections.defaultdict(int)

            while (
                sum(job_db.count_by_status(statuses=["not_started", "created", "queued", "running", "downloading"]).values()) > 0
                and not self._stop_thread
            ):
                print(f"Job status histogram: {job_db.count_by_status()}. Run stats: {dict(stats)}")
                self._job_update_loop(job_db=job_db, start_job=start_job)
                stats["run_jobs loop"] += 1

                logger.info(f"Job status histogram: {job_db.count_by_status()}. Run stats: {dict(stats)}")
                # Do sequence of micro-sleeps to allow for quick thread exit
                for _ in range(int(max(1, self.poll_sleep))):
                    time.sleep(1)
                    if self._stop_thread:
                        break

        self._thread = Thread(target=run_loop)
        self._thread.start()

    def run_jobs(self, df = None, start_job = _start_job_default, job_db = None, **kwargs,):
        # Backwards compatibility for deprecated `output_file` argument
        if "output_file" in kwargs:
            if job_db is not None:
                raise ValueError("Only one of `output_file` and `job_db` should be provided")
            warnings.warn(
                "The `output_file` argument is deprecated. Use `job_db` instead.", DeprecationWarning, stacklevel=2
            )
            job_db = kwargs.pop("output_file")
        assert not kwargs, f"Unexpected keyword arguments: {kwargs!r}"

        if isinstance(job_db, (str, Path)):
            job_db = get_job_db(path=job_db)

        if not isinstance(job_db, JobDatabaseInterface):
            raise ValueError(f"Unsupported job_db {job_db!r}")

        if job_db.exists():
            # Resume from existing db
            logger.info(f"Resuming `run_jobs` from existing {job_db}")
        elif df is not None:
            # TODO: start showing deprecation warnings for this usage pattern?
            job_db.initialize_from_df(df)

        # TODO: support user-provided `stats`
        stats = collections.defaultdict(int)

        while sum(job_db.count_by_status(statuses=["not_started", "created", "queued", "running", "downloading"]).values()) > 0:
            self._job_update_loop(job_db=job_db, start_job=start_job, stats=stats)
            stats["run_jobs loop"] += 1

            # Show current stats and sleep
            logger.info(f"Job status histogram: {job_db.count_by_status()}. Run stats: {dict(stats)}")
            time.sleep(self.poll_sleep)
            stats["sleep"] += 1

        return stats

def is_notebook() -> bool:
    """
    Determines if the code is being executed within a Jupyter notebook
    or a similar interactive environment. This function examines the
    class name of the current IPython shell to infer the execution
    context and return a boolean indicating if the environment is either
    a Jupyter notebook, qtconsole, or a terminal-based IPython shell.

    :return: True if the environment is a Jupyter notebook or qtconsole,
             otherwise False.
    """
    from IPython import get_ipython
    try:
        shell = get_ipython().__class__.__name__
        if shell == 'ZMQInteractiveShell':
            return True   # Jupyter notebook or qtconsole
        elif shell == 'TerminalInteractiveShell':
            return False  # Terminal running IPython
        else:
            return False  # Other type (?)
    except NameError:
        return False      # Probably standard Python interpreter

def add_cost_to_csv(connection: openeo.Connection, file_path: str):
    """
    Adds a 'cost' column to the CSV file at the specified file path,
    by retrieving the costs for each job ID from the given OpenEO
    connection. It reads the CSV content into a DataFrame, iterates
    through its rows, retrieves job costs using the OpenEO connection,
    and updates the CSV file with the new cost values.

    :param connection:
        An OpenEO connection object that is used to connect to the OpenEO
        backend and retrieve job information.
    :param file_path:
        The path to the CSV file where job IDs and their costs will be
        stored. It must be a valid file path string pointing to a CSV
        file on the local file system.
    :return:
        None. The function updates the provided CSV file in place by
        adding the retrieved cost information for each job ID.
    """
    df = pd.read_file(file_path)
    for i, row in df.iterrows():
        job = connection.job(row["id"])
        try:
            cost = job.describe_job()["costs"]
        except KeyError:
            cost = None
        df.loc[i, "cost"] = cost
    df.to_csv(file_path)

def check_reason(log_text: str) -> Union[str, bool]:
    """
    Analyzes the provided log text to determine the reason for specific
    errors or issues encountered within the system. This function scans
    through the log text to match predefined error patterns. If a match
    is found, it returns a corresponding string that denotes the nature
    of the error. If no recognizable pattern is found, it returns False.

    :param log_text: The log text to be analyzed for determining error
                     reasons within the system. Expected to contain
                     strings that match specific error patterns defined
                     in the function.
    :return: A string representing the identified error reason or False
             if no recognizable error pattern is detected. Possible
             return values include "OOM" for out-of-memory errors,
             "NoDataAvailable", "orfeo_error", "no_VH_band", and
             "no_tiff_in_S1".
    """
    if (re.search('Failed to allocate memory for image', log_text) or
            re.search("OOM", log_text) or re.search("exit code: 50", log_text)):
        return "OOM"
    if re.search("NoDataAvailable", log_text):
        return "NoDataAvailable"
    if re.search("Orfeo toolbox.", log_text):
        return "orfeo_error"
    if re.search("No tiff for band VH", log_text):
        return "no_VH_band"
    if re.search("sar_backscatter: No tiffs found in", log_text):
        return "no_tiff_in_S1"
    return False

def get_AOI_interactive(map_center: Tuple[float, float] = (51.22, 5.08), zoom: int = 16) -> None:
    """
    Display an interactive map within a Jupyter notebook environment, enabling users to draw and export areas
    of interest (AOI) as GeoJSON files. The map is initialized with specified geographical coordinates as its
    center and a customizable zoom level. Additional toolsets provide options for drawing polygons and
    rectangles while disabling other shapes like circles, markers, or polylines.

    :param map_center: Tuple of geographical coordinates (latitude, longitude) for setting the center of the map.
    :param zoom: Zoom level for the map where higher values indicate closer zoom.
    :return: None
    """
    if is_notebook():
        import folium
        from folium.plugins import Draw

        # Initialize the Folium map
        m = folium.Map(location=map_center, zoom_start=zoom)

        # Add drawing tools
        draw = Draw(
            export=True,  # Enable export tool
            filename="polygon.geojson",
            draw_options={
                "polyline": False,  # Disable polyline drawing
                "circle": False,  # Disable circle drawing
                "circlemarker": False,  # Disable circle marker drawing
                "polygon": True,  # Enable polygon drawing
                "rectangle": True,  # Enable rectangle drawing
                "marker": False  # Disable marker drawing
            },
        )

        # Add the drawing tools to the map
        draw.add_to(m)

        print('Please draw the AOI and press the EXPORT button.')
        return m
    else:
        print('this function is only available in a jupyter notebook')

def create_job_dataframe(gdf: gpd.GeoDataFrame, year: int, file_name_base: str, processing_type: str,
                         discriminator: Optional[str] = None, target_crs: Optional[int] = None, version: Optional[str] = None,
                         model_urls: Optional[List[str]] = None, output_band_names: Optional[List[str]] = None,
                         storage_options: Optional[storage_option_format] = None,
                         organization_id : Optional[int] = None) -> gpd.GeoDataFrame:

    '''
    gdf: geo pandas dataframe create with the foramt of the AOI_tiler function
    param processing_type : feature_generation or EUNIS habitat probabilities @Marcel this could then be extended to more if needed
                    or made more general
    organization_id : int (4digit) that represents the organization under which the costs should be booked
    discriminator : str an columns in de the gdf which will be used as an extra discriminator eg zone_name eg hEUNIS step5....ipynb
    '''

    columns = ['name', 'tileID', 'target_epsg', 'bbox', 'file_prefix', 'start_date', 'end_date', 's3_prefix','organization_id', 'geometry']
    dtypes = {'name': 'string', 'tileID': 'string', 'target_epsg': 'UInt16',
              'file_prefix': 'string', 'start_date': 'string', 'end_date': 'string',
              's3_prefix': 'string','geometry': 'geometry', 'bbox': 'string', 'organization_id':'UInt16'}

    job_df = gdf.copy()

    # evaluate if tileID is given by 'name' or 'grid20id'
    if 'grid20id' in job_df.columns:
        tile_col = 'grid20id'
    else:
        tile_col = 'name'

    # the time context is given by start and end date
    job_df['start_date'] = f'{year}-01-01'
    job_df['end_date'] = f'{year+1}-01-01'  # the end is always exclusive

    #organization ID is the same for all rows
    job_df['organization_id'] = organization_id

    # set the target epsg
    if target_crs is None:
        job_df['target_epsg'] = job_df.apply(lambda row: int(string_to_dict(row['bbox_dict'])['crs']), axis=1)
    else:
        job_df['target_epsg'] = target_crs

    job_df['bbox'] = job_df['bbox_dict']

    # set the s3_prefix which is needed for the path to S3 storage relative to bucket if we export
    if storage_options:
        job_df['s3_prefix'] = storage_options.get('S3_prefix', None)
    else:
        job_df['s3_prefix'] = None

    # a fix since the "name" column has to be unique
    job_df['tileID'] = job_df[tile_col].copy()
    if discriminator:
        job_df['name'] = job_df[tile_col] + f'_{year}' + f'_' + job_df[discriminator]
    else:
        job_df['name'] = job_df[tile_col] + f'_{year}'

    #get version
    if version:
        version = f'_{version}'

    #now we'll add some process dependant parameters
    if processing_type == 'feature_generation':
        # adding the output file name pre-fix
        if discriminator:
            job_df['file_prefix'] = job_df.apply(lambda row: f'{file_name_base}_feature-cube_year{year}_{row[tile_col]}_{row[discriminator]}'+ version, axis=1)
        else: job_df['file_prefix'] = job_df.apply(lambda row: f'{file_name_base}_feature-cube_year{year}_{row[tile_col]}'+ version, axis=1)

    elif processing_type.lower() == 'eunis_habitat_probabilities':
        if discriminator:
            job_df['file_prefix'] = job_df.apply(
                lambda row: f'{file_name_base}_EUNIS-habitat-proba-cube_year{year}_{row[tile_col]}_{row[discriminator]}' + version, axis=1)
        else: job_df['file_prefix'] = job_df.apply(
                lambda row: f'{file_name_base}_EUNIS-habitat-proba-cube_year{year}_{row[tile_col]}' + version, axis=1)
        # adding the model_urls and output_band_names (all the same for all tiles)
        job_df['model_urls'] = [model_urls] * len(job_df)
        job_df['output_band_names'] = [output_band_names] * len(job_df)
        #update dtypes dict
        columns = ['name', 'tileID', 'target_epsg', 'bbox', 'file_prefix', 'start_date', 'end_date', 's3_prefix', 'organization_id',
                   'model_urls', 'output_band_names', 'geometry']
        dtypes.update({'model_urls':'string','output_band_names':'string'})
    else:
        logger.error(f"{processing_type} is not an implemented option for processing_type. Please extended the function"+
                     f"or use feature_generation or EUNIS_habitat_probabilities")



    return job_df[columns].astype(dtypes)