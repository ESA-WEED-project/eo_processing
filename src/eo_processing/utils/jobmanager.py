import os
import json
import logging
from threading import Thread
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
from typing import Optional, Mapping
import openeo
import warnings


logger = logging.getLogger(__name__)

class WeedJobManager(MultiBackendJobManager):

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
        "cost": _ColumnProperties(dtype="float"),
    }

    def __init__(self, poll_sleep=5, root_dir='.', viz=False, max_attempts = 3, viz_labels=False):
        super().__init__(poll_sleep=poll_sleep, root_dir=root_dir)
        self.viz = viz
        self.viz_labels = viz_labels
        self.max_attempts = max_attempts
        self._cancel_download_after = (
            datetime.timedelta(seconds=1800)  )

    def download_job_too_long(self, job, row):
        """Cancel the job if it has been running for too long."""
        job_running_start_time = rfc3339.parse_datetime(row["running_start_time"], with_timezone=True)

        elapsed = datetime.datetime.now(tz=datetime.timezone.utc) - (job_running_start_time + datetime.timedelta(seconds=int(row['duration'].split(' ')[0])))
        if elapsed > self._cancel_download_after*row['attempt']:
            f"download of job {job.job_id} (after {elapsed}) has been labeled as failed.)"
            return False
        else: return False

    def check_finished(self, job):
        job_metadata = job.describe()
        title = os.path.splitext(job_metadata['title'])[0]
        metadata_path = self.get_job_metadata_path(job.job_id, title)
        return os.path.exists(metadata_path)




    def get_job_dir(self, job_id: str) -> Path:
        """Path to directory where job metadata, results and error logs are be saved."""
        return self._root_dir

    def get_error_log_path(self, job_id: str, title: str  = None) -> Path:
        """Path where error log file for the job is saved."""
        path  = self.get_job_dir(job_id) / "errors" / f"{title}_{job_id}_errors.json"
        if not path.parent.exists():
            path.parent.mkdir()
        return path

    def get_job_metadata_path(self, job_id: str, title: str  = None) -> Path:
        """Path where job metadata file is saved."""
        path = self.get_job_dir(job_id) / "metadata" / f"{title}_{job_id}_metadata.json"
        if not path.parent.exists():
            path.parent.mkdir()
        return path

    def get_job_graph_path(self, job_id: str, title: str  = None) -> Path:
        """Path where job metadata file is saved."""
        path =  self.get_job_dir(job_id) / "jobs" / f"{title}_{job_id}_job.json"
        if not path.parent.exists():
            path.parent.mkdir()
        return path

    def on_job_error(self, job, row):
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

        with open(job_graph_path, "w", encoding='utf8') as f:
            json.dump(job_metadata, f, ensure_ascii=False, indent=2)

        return check_reason(json.dumps(error_logs, ensure_ascii=False))

    def on_job_done(self, job, row):
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


    def _track_statuses(self, job_db: JobDatabaseInterface, stats: Optional[dict] = None):
        """
        Tracks status (and stats) of running jobs (in place).
        Optionally cancels jobs when running too long.
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

                logger.info(
                    f"Status of job {job_id!r} (on backend {backend_name}) is {new_status!r} (previously {previous_status!r})"
                )

                if new_status == "finished" and previous_status != "downloading":
                    stats["job finished"] += 1
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
                        active.loc[i, "attempt"] += 1
                        new_status = "not_started"
                    else:
                        new_status = "error_openeo"


                if previous_status in {"created", "queued"} and new_status == "running":
                    stats["job started running"] += 1
                    active.loc[i, "running_start_time"] = rfc3339.utcnow()

                if new_status == "canceled":
                    stats["job canceled"] += 1
                    self.on_job_cancel(the_job, active.loc[i])
                    if active.loc[i, "attempt"] <= self.max_attempts:
                        active.loc[i, "attempt"] += 1
                        new_status = "not_started"

                if new_status == "downloading":
                    if self.download_job_too_long(the_job, active.loc[i]):
                        #retry download
                        if active.loc[i, "attempt"] <= self.max_attempts+2:
                            new_status = "running"
                        else:
                            new_status = "error_downloading"





                if self._cancel_running_job_after and new_status == "running":
                    self._cancel_prolonged_job(the_job, active.loc[i])

                active.loc[i, "status"] = new_status

                # TODO: there is well hidden coupling here with "cpu", "memory" and "duration" from `_normalize_df`
                for key in job_metadata.get("usage", {}).keys():
                    if key in active.columns:
                        active.loc[i, key] = _format_usage_stat(job_metadata, key)

            except OpenEoApiError as e:
                stats["job tracking error"] += 1
                print(f"error for job {job_id!r} on backend {backend_name}")
                print(e)



        stats["job_db persist"] += 1
        job_db.persist(active)



        if self.viz:
            self.create_viz_status(job_db)

    def _launch_job(self, start_job, df, i, backend_name, stats: Optional[dict] = None):
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
                                df.loc[i, "attempt"] += 1
                                df.loc[i, "status"] = "not_started"
                            stats["job start error"] += 1
            else:
                # you can skip jobs before sending to openeo: eg some local processing needs to be finished before
                # being able to process
                df.loc[i, "status"] = "skipped"
                stats["start_job skipped"] += 1

    def create_viz_status(self, job_db : JobDatabaseInterface ):
        import matplotlib.pyplot as plt
        if is_notebook():
            from IPython.display import clear_output
            clear_output(wait=True)

        status_df = job_db.df.copy()
        #status_df.set_crs('epsg:4326',allow_override=True)

        # updating the colors for processing status
        color_dict = {"not_started": 'grey',
                      "created": 'gold',
                      "queued": 'lightsteelblue',
                      "running": 'navy',
                      "start_failed": 'salmon',
                      "skipped": 'darkorange',
                      "downloading": 'lightgreen',
                      "finished": 'green',
                      "download_error" : "lightred",
                      "error": 'darkred',
                      "canceled": 'magenta'}
        status_df['color'] = status_df['status'].map(color_dict)

        # plot the tiles with their status color
        fig, ax = plt.subplots()
        status_df.plot(ax=ax, edgecolor='black', color=status_df['color'])

        # add labels to the tiles showing the tileID and status
        if self.viz_labels:
            status_df['coords'] = status_df['geometry'].apply(lambda x: x.representative_point().coords[:])
            status_df['coords'] = [coords[0] for coords in status_df['coords']]
            for idx, row in status_df.iterrows():
                plt.annotate(text=f'{row['name']} \n ({row['status']})', xy=row['coords'],
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

def add_cost_to_csv(
    connection: openeo.Connection,
    file_path: str,
):
    df = pd.read_file(file_path)
    for i, row in df.iterrows():
        job = connection.job(row["id"])
        try:
            cost = job.describe_job()["costs"]
        except KeyError:
            cost = None
        df.loc[i, "cost"] = cost
    df.to_csv(file_path)

def check_reason(log_text):
    if re.search('Failed to allocate memory for image', log_text) or re.search("OOM", log_text) or re.search("exit code: 50", log_text):
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

