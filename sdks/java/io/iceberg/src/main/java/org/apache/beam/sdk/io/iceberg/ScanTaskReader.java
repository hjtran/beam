/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.io.iceberg;

import static org.apache.beam.sdk.io.iceberg.IcebergUtils.icebergRecordToBeamRow;
import static org.apache.beam.sdk.io.iceberg.IcebergUtils.icebergSchemaToBeamSchema;
import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Queue;
import javax.annotation.Nullable;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.data.GenericDeleteFilter;
import org.apache.iceberg.data.IdentityPartitionConverters;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.avro.DataReader;
import org.apache.iceberg.data.orc.GenericOrcReader;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.encryption.InputFilesDecryptor;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.mapping.NameMappingParser;
import org.apache.iceberg.orc.ORC;
import org.apache.iceberg.parquet.Parquet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ScanTaskReader extends BoundedSource.BoundedReader<Row> {
  private static final Logger LOG = LoggerFactory.getLogger(ScanTaskReader.class);

  private final ScanTaskSource source;
  private final Schema beamSchema;

  transient @Nullable FileIO io;
  transient @Nullable InputFilesDecryptor decryptor;
  transient @Nullable Queue<FileScanTask> fileScanTasks;
  transient @Nullable CloseableIterator<Record> currentIterator;
  transient @Nullable Record current;

  public ScanTaskReader(ScanTaskSource source) {
    this.source = source;
    this.beamSchema = icebergSchemaToBeamSchema(source.getScanConfig().getProjectedSchema());
  }

  @Override
  public boolean start() throws IOException {
    Table table = source.getTable();
    EncryptionManager encryptionManager = table.encryption();

    current = null;
    io = table.io();
    decryptor = new InputFilesDecryptor(source.getTask(), io, encryptionManager);
    fileScanTasks = new ArrayDeque<>();
    fileScanTasks.addAll(source.getTask().files());

    return advance();
  }

  @Override
  public boolean advance() throws IOException {
    Queue<FileScanTask> fileScanTasks =
        checkStateNotNull(this.fileScanTasks, "files null in advance() - did you call start()?");
    InputFilesDecryptor decryptor =
        checkStateNotNull(this.decryptor, "decryptor null in adance() - did you call start()?");

    // This nullness annotation is incorrect, but the most expedient way to work with Iceberg's APIs
    // which are not null-safe.
    org.apache.iceberg.Schema requiredSchema = source.getScanConfig().getRequiredSchema();
    @Nullable
    String nameMapping = source.getTable().properties().get(TableProperties.DEFAULT_NAME_MAPPING);

    do {
      // If our current iterator is working... do that.
      if (currentIterator != null && currentIterator.hasNext()) {
        current = currentIterator.next();
        return true;
      }

      // Close out the current iterator and try to open a new one
      if (currentIterator != null) {
        currentIterator.close();
        currentIterator = null;
      }

      LOG.info("Trying to open new file.");
      if (fileScanTasks.isEmpty()) {
        LOG.info("We have exhausted all available files in this CombinedScanTask");
        break;
      }

      // We have a new file to start reading
      FileScanTask fileTask = fileScanTasks.remove();
      DataFile file = fileTask.file();
      InputFile input = decryptor.getInputFile(fileTask);
      Map<Integer, ?> idToConstants =
          ReadUtils.constantsMap(
              fileTask, IdentityPartitionConverters::convertConstant, requiredSchema);

      CloseableIterable<Record> iterable;
      switch (file.format()) {
        case ORC:
          LOG.info("Preparing ORC input");
          ORC.ReadBuilder orcReader =
              ORC.read(input)
                  .split(fileTask.start(), fileTask.length())
                  .project(requiredSchema)
                  .createReaderFunc(
                      fileSchema ->
                          GenericOrcReader.buildReader(requiredSchema, fileSchema, idToConstants))
                  .filter(fileTask.residual());

          if (nameMapping != null) {
            orcReader.withNameMapping(NameMappingParser.fromJson(nameMapping));
          }

          iterable = orcReader.build();
          break;
        case PARQUET:
          LOG.info("Preparing Parquet input.");
          Parquet.ReadBuilder parquetReader =
              Parquet.read(input)
                  .split(fileTask.start(), fileTask.length())
                  .project(requiredSchema)
                  .createReaderFunc(
                      fileSchema ->
                          GenericParquetReaders.buildReader(
                              requiredSchema, fileSchema, idToConstants))
                  .filter(fileTask.residual());

          if (nameMapping != null) {
            parquetReader.withNameMapping(NameMappingParser.fromJson(nameMapping));
          }

          iterable = parquetReader.build();
          break;
        case AVRO:
          LOG.info("Preparing Avro input.");
          Avro.ReadBuilder avroReader =
              Avro.read(input)
                  .split(fileTask.start(), fileTask.length())
                  .project(requiredSchema)
                  .createReaderFunc(
                      fileSchema -> DataReader.create(requiredSchema, fileSchema, idToConstants));

          if (nameMapping != null) {
            avroReader.withNameMapping(NameMappingParser.fromJson(nameMapping));
          }

          iterable = avroReader.build();
          break;
        default:
          throw new UnsupportedOperationException("Cannot read format: " + file.format());
      }
      GenericDeleteFilter deleteFilter =
          new GenericDeleteFilter(
              checkStateNotNull(io), fileTask, fileTask.schema(), requiredSchema);
      iterable = deleteFilter.filter(iterable);

      iterable = ReadUtils.maybeApplyFilter(iterable, source.getScanConfig());
      currentIterator = iterable.iterator();
    } while (true);

    return false;
  }

  @Override
  public Row getCurrent() throws NoSuchElementException {
    if (current == null) {
      throw new NoSuchElementException();
    }
    return icebergRecordToBeamRow(beamSchema, current);
  }

  @Override
  public void close() throws IOException {
    if (currentIterator != null) {
      currentIterator.close();
      currentIterator = null;
    }
    if (fileScanTasks != null) {
      fileScanTasks.clear();
      fileScanTasks = null;
    }
    if (io != null) {
      io.close();
      io = null;
    }
  }

  @Override
  public BoundedSource<Row> getCurrentSource() {
    return source;
  }
}
