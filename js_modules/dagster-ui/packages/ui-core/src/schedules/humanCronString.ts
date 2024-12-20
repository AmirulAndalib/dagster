import cronstrue from 'cronstrue';
import memoize from 'lodash/memoize';

import {timezoneAbbreviation} from '../app/time/browserTimezone';

const formatOptions = memoize((language: string) => {
  const date = new Date();
  const timeString = date.toLocaleTimeString(language);
  const use24HourTimeFormat = !timeString.endsWith('AM') && !timeString.endsWith('PM');
  return {use24HourTimeFormat};
});

type CronTimezoneConfig = {
  longTimezoneName?: string;
  tzOffset?: number;
};

const convertSingleCronString = (cronSchedule: string, timezoneConfig?: CronTimezoneConfig) => {
  const {longTimezoneName, tzOffset} = timezoneConfig || {};
  let human = convertString(cronSchedule, tzOffset);

  if (longTimezoneName) {
    // Find the "At XX:YY" string and insert the timezone abbreviation.
    const timeMatch = human.match(/[0-9]{1,2}:[0-9]{2}( [A|P]M)?/g);
    if (timeMatch) {
      let shortTimezone: string | null;
      try {
        shortTimezone = timezoneAbbreviation(longTimezoneName);
      } catch {
        // Failed to extract a timezone abbreviation. Skip rendering the timezone.
        shortTimezone = null;
      }

      if (timeMatch.length && shortTimezone) {
        timeMatch.forEach((stringMatch) => {
          human = human.replace(stringMatch, `${stringMatch} ${shortTimezone}`);
        });
        return human;
      }
    }
  }

  return human;
};

export const humanCronString = (cronSchedule: string, timezoneConfig?: CronTimezoneConfig) => {
  const cronArray = cronScheduleToArray(cronSchedule);
  return cronArray
    .map((singleCron) => convertSingleCronString(singleCron, timezoneConfig))
    .join('; ');
};

const cronScheduleToArray = (cronSchedule: string) => {
  // The supplied string, if a cron union, will use single quotes for the array
  // elements. This is not valid JSON, so try to make it valid.
  const swapQuotes = cronSchedule.replace(/'/g, '"');

  try {
    const parsed = JSON.parse(swapQuotes);
    if (Array.isArray(parsed)) {
      return parsed;
    }
  } catch {
    // Fall through.
  }

  // It's just a string, or otherwise invalid. Wrap and return it.
  return [cronSchedule];
};

// NOTE: Days may be incorrect for timezone offsets.
// https://github.com/bradymholt/cRonstrue/issues/313
const convertString = (cronSchedule: string, tzOffset?: number) => {
  const standardCronString = convertIfSpecial(cronSchedule);
  try {
    return cronstrue.toString(standardCronString, {
      ...formatOptions(navigator.language),
      tzOffset,
    });
  } catch {
    return 'Invalid cron string';
  }
};

// https://en.wikipedia.org/wiki/Cron#Nonstandard_predefined_scheduling_definitions
const convertIfSpecial = (maybeSpecial: string) => {
  switch (maybeSpecial) {
    case '@yearly':
    case '@annually':
      return '0 0 1 1 *';
    case '@monthly':
      return '0 0 1 * *';
    case '@weekly':
      return '0 0 * * 0';
    case '@daily':
    case '@midnight':
      return '0 0 * * *';
    case '@hourly':
      return '0 * * * *';
    default:
      return maybeSpecial;
  }
};
