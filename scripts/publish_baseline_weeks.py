"""Explicitly consolidate the five weeks declared by the captured exercise baseline."""

import json

from health_pipeline.control import weekly_scope
from health_pipeline.weekly_publication import freeze,publish


def main():
    for week in ('2026-01-05','2026-01-12','2026-01-19','2026-01-26','2026-02-02'):
        publication=freeze(weekly_scope(week_start=week),'baseline-weekly-v1/'+week,
                           'exercise_adapter','baseline-2026-01-v1')
        result=publish(publication)
        print(json.dumps({'week_start':week,**result}),flush=True)


if __name__=='__main__':
    main()
