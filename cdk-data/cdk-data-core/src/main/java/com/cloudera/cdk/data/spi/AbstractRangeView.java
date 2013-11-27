/*
 * Copyright 2013 Cloudera.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cloudera.cdk.data.spi;

import com.cloudera.cdk.data.DatasetDescriptor;
import com.cloudera.cdk.data.Marker;
import com.cloudera.cdk.data.View;
import com.google.common.base.Objects;

import javax.annotation.concurrent.Immutable;

@Immutable
public abstract class AbstractRangeView<E> implements View<E> {

  // this isn't strictly necessary, but it is good for logs and exceptions
  protected final String datasetName;
  protected final DatasetDescriptor descriptor;
  protected final MarkerRange range;

  // This class is Immutable and must be thread-safe
  protected final ThreadLocal<Key> keys;

  protected AbstractRangeView(
      final String datasetName, final DatasetDescriptor descriptor) {
    this.datasetName = datasetName;
    this.descriptor = descriptor;
    if (descriptor.isPartitioned()) {
      this.range = new MarkerRange(new MarkerComparator(
          descriptor.getPartitionStrategy()));
      this.keys = new ThreadLocal<Key>() {
        @Override
        protected Key initialValue() {
          return new Key(descriptor.getPartitionStrategy());
        }
      };
    } else {
      // use UNDEFINED, which handles inappropriate calls to range methods
      this.range = MarkerRange.UNDEFINED;
      this.keys = null; // not used
    }
  }

  protected AbstractRangeView(AbstractRangeView<E> view, MarkerRange range) {
    this.datasetName = view.datasetName;
    this.descriptor = view.descriptor;
    this.range = range;
    // thread-safe, so okay to reuse when views share a partition strategy
    this.keys = view.keys;
  }

  protected abstract AbstractRangeView<E> newLimitedCopy(MarkerRange subRange);

  @Override
  public DatasetDescriptor getDescriptor() {
    return descriptor;
  }

  @Override
  public boolean deleteAll() {
    throw new UnsupportedOperationException(
        "This Dataset does not support deletion");
  }

  @Override
  public boolean contains(E entity) {
    if (descriptor.isPartitioned()) {
      return range.contains(keys.get().reuseFor(entity));
    } else {
      return true;
    }
  }

  @Override
  public boolean contains(Marker marker) {
    return range.contains(marker);
  }

  @Override
  public View<E> from(Marker start) {
    return newLimitedCopy(range.from(start));
  }

  @Override
  public View<E> fromAfter(Marker start) {
    return newLimitedCopy(range.fromAfter(start));
  }

  @Override
  public View<E> to(Marker end) {
    return newLimitedCopy(range.to(end));
  }

  @Override
  public View<E> toBefore(Marker end) {
    return newLimitedCopy(range.toBefore(end));
  }

  @Override
  public View<E> of(Marker partial) {
    return newLimitedCopy(range.of(partial));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if ((o == null) || !Objects.equal(this.getClass(), o.getClass())) {
      return false;
    }

    AbstractRangeView that = (AbstractRangeView) o;
    return (Objects.equal(this.descriptor, that.descriptor) &&
        Objects.equal(this.range, that.range));
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(getClass(), descriptor, range);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("dataset", datasetName)
        .add("descriptor", descriptor)
        .add("range", range)
        .toString();
  }
}
