/*
 * Copyright 2018 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.bigtable.admin.v2.stub;

import com.google.api.core.ApiFunction;
import com.google.api.core.InternalApi;
import com.google.api.gax.grpc.GrpcCallSettings;
import com.google.api.gax.grpc.GrpcCallableFactory;
import com.google.api.gax.grpc.ProtoOperationTransformers.MetadataTransformer;
import com.google.api.gax.grpc.ProtoOperationTransformers.ResponseTransformer;
import com.google.api.gax.longrunning.OperationSnapshot;
import com.google.api.gax.longrunning.OperationTimedPollAlgorithm;
import com.google.api.gax.retrying.RetrySettings;
import com.google.api.gax.rpc.ClientContext;
import com.google.api.gax.rpc.OperationCallSettings;
import com.google.api.gax.rpc.OperationCallable;
import com.google.api.gax.rpc.UnaryCallSettings;
import com.google.api.gax.rpc.UnaryCallable;
import com.google.bigtable.admin.v2.OptimizeRestoredTableMetadata;
import com.google.bigtable.admin.v2.TableName;
import com.google.longrunning.Operation;
import com.google.protobuf.Empty;
import io.grpc.MethodDescriptor;
import io.grpc.MethodDescriptor.Marshaller;
import io.grpc.MethodDescriptor.MethodType;
import java.io.IOException;
import java.io.InputStream;
import org.threeten.bp.Duration;

/**
 * Extension of the autogenerated {@link GrpcBigtableTableAdminStub}. It acts as a decorator to add
 * enhanced abilities to the autogenerated stub.
 *
 * <p>This class is considered an internal implementation detail and not meant to be used by
 * applications.
 */
@InternalApi
public class EnhancedBigtableTableAdminStub extends GrpcBigtableTableAdminStub {
  private final BigtableTableAdminStubSettings settings;
  private final ClientContext clientContext;

  private final AwaitReplicationCallable awaitReplicationCallable;
  private final OperationCallable<Void, Empty, OptimizeRestoredTableMetadata>
      optimizeRestoredTableOperationBaseCallable;

  public static EnhancedBigtableTableAdminStub createEnhanced(
      BigtableTableAdminStubSettings settings) throws IOException {
    return new EnhancedBigtableTableAdminStub(settings, ClientContext.create(settings));
  }

  private EnhancedBigtableTableAdminStub(
      BigtableTableAdminStubSettings settings, ClientContext clientContext) throws IOException {
    super(settings, clientContext);

    this.settings = settings;
    this.clientContext = clientContext;
    this.awaitReplicationCallable = createAwaitReplicationCallable();
    this.optimizeRestoredTableOperationBaseCallable =
        createOptimizeRestoredTableOperationBaseCallable();
  }

  private AwaitReplicationCallable createAwaitReplicationCallable() {
    // TODO(igorbernstein2): expose polling settings
    RetrySettings pollingSettings =
        RetrySettings.newBuilder()
            // use overall timeout from checkConsistencyCallable
            // NOTE: The overall timeout might exceed this value due to underlying retries
            .setTotalTimeout(
                settings.checkConsistencySettings().getRetrySettings().getTotalTimeout())
            // Use constant polling with jitter
            .setInitialRetryDelay(Duration.ofSeconds(10))
            .setRetryDelayMultiplier(1.0)
            .setMaxRetryDelay(Duration.ofSeconds(10))
            // These rpc timeouts are ignored, instead the rpc timeouts defined for
            // generateConsistencyToken and checkConsistency callables will be used.
            .setInitialRpcTimeout(Duration.ZERO)
            .setMaxRpcTimeout(Duration.ZERO)
            .setRpcTimeoutMultiplier(1.0)
            .build();

    return AwaitReplicationCallable.create(
        generateConsistencyTokenCallable(),
        checkConsistencyCallable(),
        clientContext,
        pollingSettings);
  }

  // Plug into gax operation infrastructure
  // gax assumes that all operations are started immediately and doesn't provide support for child
  // operations
  // this method wraps a fake method "OptimizeTable" in an OperationCallable. This should not be
  // exposed to
  // end users, but will be used for its resumeOperation functionality via a wrapper
  private OperationCallable<Void, Empty, OptimizeRestoredTableMetadata>
      createOptimizeRestoredTableOperationBaseCallable() {
    // Fake initial callable settings. Since this is child operation, it doesn't have an initial
    // callable but gax doesn't have support for child operation, so this creates a fake method
    // descriptor that will never be used.
    GrpcCallSettings<Void, Operation> unusedInitialCallSettings =
        GrpcCallSettings.create(
            MethodDescriptor.<Void, Operation>newBuilder()
                .setType(MethodType.UNARY)
                .setFullMethodName(
                    "google.bigtable.admin.v2.BigtableTableAdmin/OptimizeRestoredTable")
                .setRequestMarshaller(
                    new Marshaller<Void>() {
                      @Override
                      public InputStream stream(Void value) {
                        throw new UnsupportedOperationException("not used");
                      }

                      @Override
                      public Void parse(InputStream stream) {
                        throw new UnsupportedOperationException("not used");
                      }
                    })
                .setResponseMarshaller(
                    new Marshaller<Operation>() {
                      @Override
                      public InputStream stream(Operation value) {
                        throw new UnsupportedOperationException("not used");
                      }

                      @Override
                      public Operation parse(InputStream stream) {
                        throw new UnsupportedOperationException("not used");
                      }
                    })
                .build());

    // helpers to extract the underlying protos from the operation
    final MetadataTransformer<OptimizeRestoredTableMetadata> protoMetadataTransformer =
        MetadataTransformer.create(OptimizeRestoredTableMetadata.class);

    final ResponseTransformer<com.google.protobuf.Empty> protoResponseTransformer =
        ResponseTransformer.create(com.google.protobuf.Empty.class);

    // TODO(igorbernstein2): expose polling settings
    OperationCallSettings<Void, Empty, OptimizeRestoredTableMetadata> operationCallSettings =
        OperationCallSettings.<Void, Empty, OptimizeRestoredTableMetadata>newBuilder()
            // Since this is used for a child operation, the initial call settings will not be used
            .setInitialCallSettings(
                UnaryCallSettings.<Void, OperationSnapshot>newUnaryCallSettingsBuilder()
                    .setSimpleTimeoutNoRetries(Duration.ZERO)
                    .build())
            // Configure the extractors to wrap the protos
            .setMetadataTransformer(
                new ApiFunction<OperationSnapshot, OptimizeRestoredTableMetadata>() {
                  @Override
                  public OptimizeRestoredTableMetadata apply(OperationSnapshot input) {
                    return protoMetadataTransformer.apply(input);
                  }
                })
            .setResponseTransformer(
                new ApiFunction<OperationSnapshot, Empty>() {
                  @Override
                  public Empty apply(OperationSnapshot input) {
                    return protoResponseTransformer.apply(input);
                  }
                })
            .setPollingAlgorithm(
                OperationTimedPollAlgorithm.create(
                    RetrySettings.newBuilder()
                        .setInitialRetryDelay(Duration.ofMillis(500L))
                        .setRetryDelayMultiplier(1.5)
                        .setMaxRetryDelay(Duration.ofMillis(5000L))
                        .setInitialRpcTimeout(Duration.ZERO) // ignored
                        .setRpcTimeoutMultiplier(1.0) // ignored
                        .setMaxRpcTimeout(Duration.ZERO) // ignored
                        .setTotalTimeout(Duration.ofMillis(600000L))
                        .build()))
            .build();

    // Create the callable
    return GrpcCallableFactory.createOperationCallable(
        unusedInitialCallSettings, operationCallSettings, clientContext, getOperationsStub());
  }

  public UnaryCallable<TableName, Void> awaitReplicationCallable() {
    return awaitReplicationCallable;
  }

  public OperationCallable<Void, Empty, OptimizeRestoredTableMetadata>
      awaitOptimizeRestoredTableCallable() {
    return optimizeRestoredTableOperationBaseCallable;
  }
}
