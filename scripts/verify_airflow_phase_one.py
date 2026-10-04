"""Compatibility command: inspect complete DAG imports without processing deliveries."""

import json

from airflow.models.dagbag import DagBag


def main():
    bag=DagBag(dag_folder='/opt/airflow/dags',include_examples=False,safe_mode=False)
    if bag.import_errors:
        raise RuntimeError('DAG import errors: '+json.dumps(bag.import_errors))
    required={'wearable_daily_pipeline':'publish_daily_analytics',
              'wearable_weekly_biostatistics':'publish_week'}
    for name,last_task in required.items():
        if name not in bag.dags or last_task not in bag.dags[name].task_ids:
            raise AssertionError('Missing complete DAG: '+name)
    print(json.dumps({'dags':{name:sorted(bag.dags[name].task_ids) for name in required},
                      'import_errors':{},'read_only':True}))


if __name__=='__main__':
    main()
