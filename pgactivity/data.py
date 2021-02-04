"""
pg_activity
author: Julien Tachoires <julmon@gmail.com>
license: PostgreSQL License

Copyright (c) 2012 - 2019, Julien Tachoires
Copyright (c) 2020, Dalibo

Permission to use, copy, modify, and distribute this software and its
documentation for any purpose, without fee, and without a written
agreement is hereby granted, provided that the above copyright notice
and this paragraph and the following two paragraphs appear in all copies.

IN NO EVENT SHALL JULIEN TACHOIRES BE LIABLE TO ANY PARTY FOR DIRECT,
INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING LOST
PROFITS, ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS DOCUMENTATION,
EVEN IF JULIEN TACHOIRES HAS BEEN ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

JULIEN TACHOIRES SPECIFICALLY DISCLAIMS ANY WARRANTIES, INCLUDING, BUT
NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS
FOR A PARTICULAR PURPOSE. THE SOFTWARE PROVIDED HEREUNDER IS ON AN "AS IS"
BASIS, AND JULIEN TACHOIRES HAS NO OBLIGATIONS TO PROVIDE MAINTENANCE,
SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.
"""

import getpass
import optparse
import re
from typing import Dict, List, Optional, Tuple, Union

import attr
import psutil
import psycopg3
from psycopg3.errors import Error, InterfaceError, InvalidPassword, OperationalError
from psycopg3.connection import Connection

from . import queries
from .types import BWProcess, RunningProcess
from .utils import clean_str


def pg_get_version(pg_conn: Connection) -> str:
    """Get PostgreSQL server version."""
    query = queries.get("get_version")
    with pg_conn.cursor() as cur:
        cur.execute(query)
        ret: Tuple[str] = cur.fetchone()  # type: ignore
    return ret[0]


def pg_get_num_version(text_version: str) -> Tuple[str, int]:
    """Return PostgreSQL short & numeric version from a string (SELECT
    version()).

    >>> pg_get_num_version('PostgreSQL 11.9')
    ('PostgreSQL 11.9', 110900)
    >>> pg_get_num_version('EnterpriseDB 11.9 (Debian 11.9-0+deb10u1)')
    ('EnterpriseDB 11.9', 110900)
    >>> pg_get_num_version("PostgreSQL 13.0beta2")
    ('PostgreSQL 13.0', 130000)
    """
    res = re.match(
        r"^(PostgreSQL|EnterpriseDB) ([0-9]+)\.([0-9]+)(?:\.([0-9]+))?",
        text_version,
    )
    if res is not None:
        rmatch = res.group(2)
        if int(res.group(3)) < 10:
            rmatch += "0"
        rmatch += res.group(3)
        if res.group(4) is not None:
            if int(res.group(4)) < 10:
                rmatch += "0"
            rmatch += res.group(4)
        else:
            rmatch += "00"
        pg_version = str(res.group(0))
        pg_num_version = int(rmatch)
        return pg_version, pg_num_version
    return pg_get_num_dev_version(text_version)


def pg_get_num_dev_version(text_version: str) -> Tuple[str, int]:
    """Return PostgreSQL short & numeric devel. or beta version from a string
    (SELECT version()).

    >>> pg_get_num_dev_version("PostgreSQL 11.9devel0")
    ('PostgreSQL 11.9devel', 110900)
    """
    res = re.match(
        r"^(PostgreSQL|EnterpriseDB) ([0-9]+)(?:\.([0-9]+))?(devel|beta[0-9]+|rc[0-9]+)",
        text_version,
    )
    if not res:
        raise Exception(f"Undefined PostgreSQL version: {text_version}")
    rmatch = res.group(2)
    if res.group(3) is not None:
        if int(res.group(3)) < 10:
            rmatch += "0"
        rmatch += res.group(3)
    else:
        rmatch += "00"
    rmatch += "00"
    pg_version = str(res.group(0))
    pg_num_version = int(rmatch)
    return pg_version, pg_num_version


@attr.s(auto_attribs=True, frozen=True, slots=True)
class Data:
    pg_conn: Connection
    pg_version: str
    pg_num_version: int
    min_duration: float
    dsn_parameters: Dict[str, str]

    @classmethod
    def pg_connect(
        cls,
        min_duration: float,
        *,
        host: Optional[str] = None,
        port: int = 5432,
        user: str = "postgres",
        password: Optional[str] = None,
        database: str = "postgres",
        rds_mode: bool = False,
        service: Optional[str] = None,
        dsn: str = "",
    ) -> "Data":
        """Create an instance by connecting to a PostgreSQL server."""
        pg_conn = None
        if host is None or host == "localhost":
            # try to connect using UNIX socket
            try:
                if service is not None:
                    pg_conn = psycopg3.connect(
                        autocommit=True,
                        service=service,
                    )
                elif dsn:
                    pg_conn = psycopg3.connect(dsn, autocommit=True)
                else:
                    pg_conn = psycopg3.connect(
                        autocommit=True,
                        dbname=database,
                        user=user,
                        port=port,
                        password=password,
                    )
            except Error as psy_err:
                if host is None:
                    raise psy_err
        if pg_conn is None:  # fallback on TCP/IP connection
            if service is not None:
                pg_conn = psycopg3.connect(
                    autocommit=True,
                    service=service,
                )
            elif dsn:
                pg_conn = psycopg3.connect(dsn, autocommit=True)
            else:
                pg_conn = psycopg3.connect(
                    autocommit=True,
                    dbname=database,
                    host=host,
                    port=port,
                    user=user,
                    password=password,
                )
        if not rds_mode:  # Make sure we are using superuser if not on RDS
            with pg_conn.cursor() as cur:
                cur.execute(queries.get("is_superuser"))
                ret = cur.fetchone()
            assert ret
            if ret[0] != "on":
                raise Exception("Must be run with database superuser privileges.")

        pg_version, pg_num_version = pg_get_num_version(pg_get_version(pg_conn))
        return cls(
            pg_conn,
            pg_version,
            pg_num_version,
            min_duration=min_duration,
            dsn_parameters=pg_conn.info.get_parameters(),
        )

    def try_reconnect(self) -> Optional["Data"]:
        try:
            pg_conn = psycopg3.connect(autocommit=True, **self.dsn_parameters)
        except (InterfaceError, OperationalError):
            return None
        else:
            return attr.evolve(
                self, pg_conn=pg_conn, dsn_parameters=pg_conn.info.get_parameters()
            )

    def pg_is_local_access(self) -> bool:
        """
        Verify if the user running pg_activity can acces
        system informations for the postmaster process.
        """
        try:
            query = queries.get("get_pid_file")
            with self.pg_conn.cursor() as cur:
                cur.execute(query)
                (pid_file,) = cur.fetchone()  # type: ignore
            with open(pid_file, "r") as fd:
                pid = fd.readlines()[0].strip()
                try:
                    proc = psutil.Process(int(pid))
                    proc.io_counters()
                    proc.cpu_times()
                    return True
                except psutil.AccessDenied:
                    return False
                except Exception:
                    return False
        except Exception:
            return False

    def pg_cancel_backend(self, pid: int) -> bool:
        """
        Cancel a backend
        """
        query = queries.get("do_pg_cancel_backend")
        with self.pg_conn.cursor() as cur:
            cur.execute(query, {"pid": pid})
            (is_stopped,) = cur.fetchone()  # type: ignore
        return bool(is_stopped)

    def pg_terminate_backend(self, pid: int) -> bool:
        """
        Terminate a backend
        """
        if self.pg_num_version >= 80400:
            query = queries.get("do_pg_terminate_backend")
        else:
            query = queries.get("do_pg_cancel_backend")
        with self.pg_conn.cursor() as cur:
            cur.execute(query, {"pid": pid})
            (is_stopped,) = cur.fetchone()  # type: ignore
        return bool(is_stopped)

    DbInfoDict = Dict[str, Union[str, int, float]]

    def pg_get_db_info(
        self,
        prev_db_infos: Optional[DbInfoDict],
        using_rds: bool = False,
        skip_sizes: bool = False,
    ) -> DbInfoDict:
        """
        Get current sum of transactions, total size and  timestamp.
        """
        prev_total_size = "0"
        if prev_db_infos is not None:
            prev_total_size = prev_db_infos["total_size"]  # type: ignore

        query = queries.get("get_db_info")
        with self.pg_conn.cursor() as cur:
            cur.execute(
                query,
                {
                    "skip_db_size": skip_sizes,
                    "prev_total_size": prev_total_size,
                    "using_rds": using_rds,
                },
            )
            timestamp, no_xact, total_size, max_length = cur.fetchone()  # type: ignore
        tps = 0
        size_ev = 0.0
        if prev_db_infos is not None:
            try:
                tps = int(
                    (no_xact - prev_db_infos["no_xact"])
                    / (timestamp - prev_db_infos["timestamp"])
                )
                size_ev = float(
                    float(total_size - prev_db_infos["total_size"])
                    / (timestamp - prev_db_infos["timestamp"])
                )
            except ZeroDivisionError:
                pass
        return {
            "timestamp": timestamp,
            "no_xact": no_xact,
            "total_size": total_size,
            "max_length": max_length,
            "tps": tps,
            "size_ev": size_ev,
        }

    def pg_get_active_connections(self) -> int:
        """
        Get total of active connections.
        """

        if self.pg_num_version < 90200:
            query = queries.get("get_active_connections.sql")
        else:
            query = queries.get("get_active_connections_post_90200")

        with self.pg_conn.cursor() as cur:
            cur.execute(query)
            (active_connections,) = cur.fetchone()  # type: ignore
        return int(active_connections)

    def pg_get_activities(self, duration_mode: int = 1) -> List[RunningProcess]:
        """
        Get activity from pg_stat_activity view.
        """
        if self.pg_num_version >= 110000:
            query = queries.get("get_pg_activity_post_110000")
        elif self.pg_num_version >= 100000:
            query = queries.get("get_pg_activity_post_100000")
        elif self.pg_num_version >= 90600:
            query = queries.get("get_pg_activity_post_90600")
        elif self.pg_num_version >= 90200:
            query = queries.get("get_pg_activity_post_90200")
        elif self.pg_num_version < 90200:
            query = queries.get("get_pg_activity")

        duration_column = self.get_duration_column(duration_mode)
        query = query.format(duration_column=duration_column)

        with self.pg_conn.cursor() as cur:
            cur.execute(query, {"min_duration": self.min_duration})
            rows = cur.fetchall()

        return [RunningProcess.from_row(*row) for row in rows]

    def pg_get_waiting(self, duration_mode: int = 1) -> List[BWProcess]:
        """
        Get waiting queries.
        """
        if self.pg_num_version >= 90200:
            query = queries.get("get_waiting_post_90200")
        elif self.pg_num_version < 90200:
            query = queries.get("get_waiting")

        duration_column = self.get_duration_column(duration_mode)
        query = query.format(duration_column=duration_column)

        with self.pg_conn.cursor() as cur:
            cur.execute(query, {"min_duration": self.min_duration})
            rows = cur.fetchall()
        return [BWProcess.from_row(*row) for row in rows]

    def pg_get_blocking(self, duration_mode: int = 1) -> List[BWProcess]:
        """
        Get blocking queries
        """
        if self.pg_num_version >= 90200:
            query = queries.get("get_blocking_post_90200")
        elif self.pg_num_version < 90200:
            query = queries.get("get_blocking")

        duration_column = self.get_duration_column(duration_mode)
        query = query.format(duration_column=duration_column)

        with self.pg_conn.cursor() as cur:
            cur.execute(query, {"min_duration": self.min_duration})
            rows = cur.fetchall()
        return [BWProcess.from_row(*row) for row in rows]

    def pg_is_local(self) -> bool:
        """
        Is pg_activity connected localy ?
        """
        query = queries.get("get_pga_inet_addresses")
        with self.pg_conn.cursor() as cur:
            cur.execute(query)
            inet_server_addr, inet_client_addr = cur.fetchone()  # type: ignore
        if inet_server_addr == inet_client_addr:
            return True
        return False

    @staticmethod
    def get_duration_column(duration_mode: int = 1) -> str:
        """Return the duration column depending on duration_mode.

        >>> Data.get_duration_column(1)
        'query_start'
        >>> Data.get_duration_column(2)
        'xact_start'
        >>> Data.get_duration_column(3)
        'backend_start'
        >>> Data.get_duration_column(9)
        'query_start'
        """
        if duration_mode not in (1, 2, 3):
            duration_mode = 1
        return ["query_start", "xact_start", "backend_start"][duration_mode - 1]


def pg_connect(
    options: optparse.Values,
    dsn: str,
    password: Optional[str] = None,
    service: Optional[str] = None,
    exit_on_failed: bool = True,
    min_duration: float = 0.0,
) -> Data:
    """Try to build a Data instance by to connecting to postgres."""
    for nb_try in range(2):
        try:
            data = Data.pg_connect(
                dsn=dsn,
                host=options.host,
                port=options.port,
                user=options.username,
                password=password,
                database=options.dbname,
                rds_mode=options.rds,
                service=service,
                min_duration=min_duration,
            )
        except OperationalError as err:
            if nb_try < 1 and isinstance(err, InvalidPassword):
                password = getpass.getpass()
            elif exit_on_failed:
                msg = str(err).replace("FATAL:", "")
                raise SystemExit("pg_activity: FATAL: %s" % clean_str(msg))
            else:
                raise Exception("Could not connect to PostgreSQL")
        else:
            break
    return data
