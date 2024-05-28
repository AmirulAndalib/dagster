import {
  Box,
  ButtonGroup,
  CursorHistoryControls,
  ErrorBoundary,
  NonIdealState,
  Spinner,
} from '@dagster-io/ui-components';
import React, {useDeferredValue, useMemo} from 'react';

import {ExecutionTimeline} from './ExecutionTimeline';
import {BackfillDetailsBackfillFragment} from './types/BackfillPage.types';
import {useQueryRefreshAtInterval} from '../../app/QueryRefresh';
import {RunsFilter} from '../../graphql/types';
import {useQueryPersistedState} from '../../hooks/useQueryPersistedState';
import {RunTable} from '../../runs/RunTable';
import {DagsterTag} from '../../runs/RunTag';
import {usePaginatedRunsTableRuns} from '../../runs/RunsRoot';
import {useRunsForTimeline} from '../../runs/useRunsForTimeline';
import {StickyTableContainer} from '../../ui/StickyTableContainer';

export const BackfillRunsTab = ({backfill}: {backfill: BackfillDetailsBackfillFragment}) => {
  const [view, setView] = useQueryPersistedState<'timeline' | 'list'>({
    defaults: {view: 'timeline'},
    queryKey: 'view',
  });
  const filter: RunsFilter = useMemo(
    () => ({tags: [{key: DagsterTag.Backfill, value: backfill.id}]}),
    [backfill],
  );

  const actionBarComponents = (
    <>
      <ButtonGroup
        activeItems={new Set([view])}
        onClick={(id: 'timeline' | 'list') => {
          setView(id);
        }}
        buttons={[
          {id: 'timeline', icon: 'gantt_waterfall', label: 'Timeline'},
          {id: 'list', icon: 'list', label: 'List'},
        ]}
      />
    </>
  );

  return (
    <Box flex={{direction: 'column'}} style={{minHeight: 0}}>
      {view === 'timeline' ? (
        <ExecutionRunTimeline
          filter={filter}
          startTimestamp={backfill.timestamp}
          endTimestamp={backfill.endTimestamp}
          actionBarComponents={actionBarComponents}
        />
      ) : (
        <ExecutionRunTable filter={filter} actionBarComponents={actionBarComponents} />
      )}
    </Box>
  );
};

const ExecutionRunTable = ({
  filter,
  actionBarComponents,
}: {
  filter: RunsFilter;
  actionBarComponents: React.ReactNode;
}) => {
  const {queryResult, paginationProps} = usePaginatedRunsTableRuns(filter);
  const pipelineRunsOrError = queryResult.data?.pipelineRunsOrError;

  useQueryRefreshAtInterval(queryResult, 15000);

  if (!pipelineRunsOrError) {
    return (
      <Box padding={{vertical: 48}}>
        <Spinner purpose="page" />
      </Box>
    );
  }
  if (pipelineRunsOrError.__typename !== 'Runs') {
    return (
      <Box padding={{vertical: 64}}>
        <NonIdealState icon="error" title="Query Error" description={pipelineRunsOrError.message} />
      </Box>
    );
  }

  return (
    <StickyTableContainer $top={0}>
      <RunTable runs={pipelineRunsOrError.results} actionBarComponents={actionBarComponents} />
      {pipelineRunsOrError.results.length > 0 ? (
        <div style={{marginTop: '16px'}}>
          <CursorHistoryControls {...paginationProps} />
        </div>
      ) : null}
    </StickyTableContainer>
  );
};

const POLL_INTERVAL = 60 * 1000;
const ONE_WEEK = 7 * 24 * 60 * 60 * 1000;

const ExecutionRunTimeline = ({
  filter,
  startTimestamp,
  actionBarComponents,
  endTimestamp,
}: {
  filter: RunsFilter;
  actionBarComponents: React.ReactNode;
  startTimestamp: number;
  endTimestamp: number | null;
}) => {
  const timelineRange: [number, number] = useMemo(() => {
    return [startTimestamp * 1000, endTimestamp ? endTimestamp * 1000 : Date.now() + ONE_WEEK];
  }, [startTimestamp, endTimestamp]);

  const runsForTimelineRet = useRunsForTimeline(timelineRange, filter);

  // Use deferred value to allow paginating quickly with the UI feeling more responsive.
  const {jobs, initialLoading} = useDeferredValue(runsForTimelineRet);

  const [_, setTick] = React.useState(0);

  React.useEffect(() => {
    setTick((t) => t + 1);
    let timer: NodeJS.Timeout;
    if (!endTimestamp) {
      timer = setInterval(() => {
        setTick((t) => t + 1);
      }, POLL_INTERVAL);
    }
    return () => {
      clearInterval(timer);
    };
  }, [endTimestamp]);

  // for now, "hack" the timeline to show runs on separate rows by pretending they're jobs
  const job = jobs[0];
  const {runs, range, now} = React.useMemo(() => {
    const now = Date.now();
    if (!job) {
      return {runs: [], now, range: [now, now] as [number, number]};
    }

    const runs = job.runs;

    // Determine the minimum and maximum extent of the backfill runs
    const range: [number, number] = [
      Math.min(...runs.map((j) => j.startTime || now)),
      Math.max(endTimestamp || now, ...runs.map((j) => j.endTime || now)),
    ];

    // Pad it by 5% on each side, looks bad when it touches the edges
    const duration = range[1] - range[0];
    range[0] -= duration * 0.15;
    range[1] += duration * 0.15;

    return {runs, now, range};
  }, [job, endTimestamp]);

  if (initialLoading) {
    return (
      <Box padding={{vertical: 48}}>
        <Spinner purpose="page" />
      </Box>
    );
  }
  if (!runs.length) {
    return (
      <Box padding={48}>
        <NonIdealState title="No runs launched" />
      </Box>
    );
  }

  return (
    <>
      <Box padding={{horizontal: 24, vertical: 12}}>{actionBarComponents}</Box>
      <ErrorBoundary region="timeline">
        <ExecutionTimeline loading={initialLoading} range={range} runs={runs} now={now} />
      </ErrorBoundary>
    </>
  );
};
