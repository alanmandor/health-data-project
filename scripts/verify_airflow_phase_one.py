"""Compatibility command: inspect complete DAG imports without processing deliveries."""

import json
import argparse

from airflow.models.dagbag import DagBag


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--dag-folder', default='/opt/airflow/dags')
    args = parser.parse_args()
    bag=DagBag(dag_folder=args.dag_folder,include_examples=False,safe_mode=False)
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
