#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Create and access metrics
"""

from meerschaum.api import fast_api, endpoints, database, connector
from meerschaum.api.models import Metric
from meerschaum.api.tables import get_tables
from meerschaum.utils.misc import attempt_import
sqlalchemy = attempt_import('sqlalchemy')
endpoint = endpoints['mrsm'] + '/metrics'

@fast_api.post(endpoint)
async def register_metric(metric : Metric):
    """
    Register a new metric
    """
    query = get_tables()['metrics'].insert().values(
        metric_key = metric.metric_key,
        metric_name = metric.metric_name,
    )

    last_record_id = await database.execute(query)
    return {**metric.dict(), "metric_id": last_record_id}

@fast_api.get(endpoint)
async def get_metrics():
    query = get_tables()['metrics'].select()
    return await database.fetch_all(query)

