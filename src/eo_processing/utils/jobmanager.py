import os
import json
import logging
from openeo.util import rfc3339
from pathlib import Path
import collections
from openeo.extra.job_management import MultiBackendJobManager,_format_usage_stat, JobDatabaseInterface
from openeo.rest import OpenEoApiError
import pandas as pd
from typing import Optional
import openeo


logger = logging.getLogger(__name__)

class WeedJobManager(MultiBackendJobManager):

    def __init__(self, poll_sleep=5, root_dir='.', viz=False):
        super().__init__(poll_sleep=poll_sleep, root_dir=root_dir)
        self.viz = viz

    @staticmethod
    def _normalize_df(df: pd.DataFrame) -> pd.DataFrame:
        """
        Normalize given pandas dataframe (creating a new one):
        ensure we have the required columns.

        :param df: The dataframe to normalize.
        :return: a new dataframe that is normalized.
        """
        # check for some required columns.
        required_with_default = [
            ("status", "not_started"),
            ("id", None),
            ("start_time", None),
            ("running_start_time", None),
            # TODO: columns "cpu", "memory", "duration" are not referenced directly
            #       within MultiBackendJobManager making it confusing to claim they are required.
            #       However, they are through assumptions about job "usage" metadata in `_track_statuses`.
            #       => proposed solution: allow to configure usage columns when adding a backend
            ("cpu", None),
            ("memory", None),
            ("duration", None),
            ("backend_name", None),
            ("cost", None)
        ]
        new_columns = {col: val for (col, val) in required_with_default if col not in df.columns}
        df = df.assign(**new_columns)

        return df

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
            error_log_path.write_text(json.dumps(error_logs, indent=2))

        else:
            error_log_path.write_text(
                "Couldn't find any errors in the logs. Please check manually.")

        job_graph_path.write_text(
            json.dumps(job_metadata, indent=2))

    def on_job_done(self, job, row):
        job_metadata = job.describe()
        job_dir = self.get_job_dir(job.job_id)
        title = os.path.splitext(job_metadata['title'])[0]
        metadata_path = self.get_job_metadata_path(job.job_id,title)
        job_graph_path = self.get_job_graph_path(job.job_id,title)
        error_log_path = self.get_error_log_path(job.job_id, title)

        self.ensure_job_dir_exists(job.job_id)

        with open(metadata_path, "w", encoding='utf8') as f:
            json.dump(job_metadata, f, ensure_ascii=False)

        results = job.get_results()

        results.download_files(job_dir, include_stac_metadata=False)

        job_graph_path.write_text(json.dumps(job_metadata, indent=2))

        logs = job.logs(level="error")

        if len(logs) > 0:
            self.ensure_job_dir_exists(job.job_id)
            error_log_path.write_text(json.dumps(logs, indent=2))

        return row

    def _track_statuses(self, job_db: JobDatabaseInterface, stats: Optional[dict] = None):
        """
        Tracks status (and stats) of running jobs (in place).
        Optionally cancels jobs when running too long.
        """
        stats = stats if stats is not None else collections.defaultdict(int)

        active = job_db.get_by_status(statuses=["created", "queued", "running"])

        if self.viz:
            self.create_viz_status(job_db)

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

                if new_status == "finished":
                    stats["job finished"] += 1
                    self.on_job_done(the_job, active.loc[i])
                    active.loc[i, "cost"] = job_metadata['costs']

                if previous_status != "error" and new_status == "error":
                    stats["job failed"] += 1
                    self.on_job_error(the_job, active.loc[i])
                    new_status = "not_started"

                if previous_status in {"created", "queued"} and new_status == "running":
                    stats["job started running"] += 1
                    active.loc[i, "running_start_time"] = rfc3339.utcnow()

                if new_status == "canceled":
                    stats["job canceled"] += 1
                    self.on_job_cancel(the_job, active.loc[i])
                    new_status = "not_started"

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
                      "finished": 'lime',
                      "error": 'darkred',
                      "canceled": 'magenta'}
        status_df['color'] = status_df['status'].map(color_dict)

        # plot the tiles with their status color
        fig, ax = plt.subplots()
        status_df.plot(ax=ax, edgecolor='black', color=status_df['color'])

        # add labels to the tiles showing the tileID and status
        status_df['coords'] = status_df['geometry'].apply(lambda x: x.representative_point().coords[:])
        status_df['coords'] = [coords[0] for coords in status_df['coords']]
        for idx, row in status_df.iterrows():
            plt.annotate(text=f'{row['name']} \n ({row['status']})', xy=row['coords'],
                         horizontalalignment='center', color='k')

        # show the figure
        plt.show()


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