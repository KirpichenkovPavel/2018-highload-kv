package ru.mail.polis.kirpichenkov;

import org.jetbrains.annotations.NotNull;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;

public class NanoClock extends Clock {
  private final Clock clock;

  private final long initialNanos;

  private final Instant initialInstant;

  public NanoClock() {
    this(Clock.systemUTC());
  }

  public NanoClock(@NotNull final Clock clock) {
    this.clock = clock;
    initialInstant = clock.instant();
    initialNanos = getSystemNanos();
  }

  @Override
  @NotNull
  public ZoneId getZone() {
    return clock.getZone();
  }

  @Override
  @NotNull
  public Instant instant() {
    return initialInstant.plusNanos(getSystemNanos() - initialNanos);
  }

  @Override
  @NotNull
  public Clock withZone(final ZoneId zone) {
    return new NanoClock(clock.withZone(zone));
  }

  private long getSystemNanos() {
    return System.nanoTime();
  }
}
