# REddyProc Local-Time Calendar Policy

This note explains a small but important design choice in the Python `miaproc`
re-implementation of the R workflow.

## How It Worked Before

In the R script `R_manglaria.R`, the eddy covariance timestamps are converted to
local site time before deriving the calendar fields used by REddyProc:

```r
DateTime_Local = with_tz(DateTime, "America/Mazatlan")
Year = year(DateTime_Local)
DoY = yday(DateTime_Local)
Hour = hour(DateTime_Local) + minute(DateTime_Local) / 60
```

This was correct for the validated Marismas Nacionales case study, because that
site uses the `America/Mazatlan` local timezone context.

REddyProc uses `Year`, `DoY`, and `Hour` as local-time calendar fields, including
for solar-position-related calculations. So these fields should reflect the site
timezone, not necessarily UTC.

## Why We Are Changing The Package Design

The R script was written for a specific site and case study. A package function
cannot safely hard-code `America/Mazatlan`, because that would silently bias any
future site processed with `miaproc`.

For example:

- Marismas Nacionales may need `America/Mazatlan`.
- Another site may need a different IANA timezone.
- Some data may already arrive in local time.
- Some data may arrive in UTC and need calendar fields derived in the site-local
  timezone.

Hard-coding the timezone would make the package look general while quietly
producing site-specific calendar fields.

## How We Will Handle It From Now On

The stage-2 preparation helper will accept an explicit optional timezone argument,
for example:

```python
prepare_reddyproc_input(df, local_tz="America/Mazatlan")
```

The policy is:

- The returned `DateTime` column preserves the original observation instant.
- `Year`, `DoY`, and `Hour` are derived from the site-local calendar when
  `local_tz` is provided.
- `local_tz` changes only the derived calendar fields, not the returned
  `DateTime` values.
- The package will not silently assume `America/Mazatlan`.

This keeps the Marismas Nacionales workflow faithful to the R script while making
the package safe for additional sites.

## Practical Example

For the validated Marismas Nacionales run, call:

```python
prepare_reddyproc_input(stage1_df, local_tz="America/Mazatlan")
```

For a different site, pass that site's local timezone instead.

If no timezone is provided, the helper should derive calendar fields from the
timestamps as provided and documented by the function. Naive timestamps and
timezone-aware timestamps should be handled deliberately, not by relying on the
computer's local timezone.

## Reviewer Invariant

The key behavior to preserve in tests is:

> `local_tz` may shift `Year`, `DoY`, and `Hour`, but it must not alter the
> returned `DateTime` column.

This is why the implementation includes a test like:

```text
TestCalendarFields::test_local_tz_shifts_calendar_fields_only
```

## Summary

Before:

- The R script always derived REddyProc calendar fields using
  `America/Mazatlan`.

Now:

- The package derives those fields using an explicit `local_tz` parameter.
- Marismas Nacionales can still use `America/Mazatlan`.
- Other sites can use their own timezone.
- The original timestamp is preserved.
