/*
 * Copyright 2020 Google LLC
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
package com.google.cloud.bigtable.admin.v2;

import static com.google.cloud.bigtable.admin.v2.BaseBigtableTableAdminClient.ListBackupsPagedResponse;
import static com.google.cloud.bigtable.admin.v2.BaseBigtableTableAdminClient.ListSnapshotsPagedResponse;
import static com.google.cloud.bigtable.admin.v2.BaseBigtableTableAdminClient.ListTablesPagedResponse;

import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.GaxGrpcProperties;
import com.google.api.gax.grpc.testing.LocalChannelProvider;
import com.google.api.gax.grpc.testing.MockGrpcService;
import com.google.api.gax.grpc.testing.MockServiceHelper;
import com.google.api.gax.rpc.ApiClientHeaderProvider;
import com.google.api.gax.rpc.InvalidArgumentException;
import com.google.api.gax.rpc.StatusCode;
import com.google.api.resourcenames.ResourceName;
import com.google.bigtable.admin.v2.AppProfileName;
import com.google.bigtable.admin.v2.Backup;
import com.google.bigtable.admin.v2.BackupName;
import com.google.bigtable.admin.v2.CheckConsistencyRequest;
import com.google.bigtable.admin.v2.CheckConsistencyResponse;
import com.google.bigtable.admin.v2.ClusterName;
import com.google.bigtable.admin.v2.CreateBackupRequest;
import com.google.bigtable.admin.v2.CreateTableFromSnapshotRequest;
import com.google.bigtable.admin.v2.CreateTableRequest;
import com.google.bigtable.admin.v2.DeleteBackupRequest;
import com.google.bigtable.admin.v2.DeleteSnapshotRequest;
import com.google.bigtable.admin.v2.DeleteTableRequest;
import com.google.bigtable.admin.v2.DropRowRangeRequest;
import com.google.bigtable.admin.v2.GenerateConsistencyTokenRequest;
import com.google.bigtable.admin.v2.GenerateConsistencyTokenResponse;
import com.google.bigtable.admin.v2.GetBackupRequest;
import com.google.bigtable.admin.v2.GetSnapshotRequest;
import com.google.bigtable.admin.v2.GetTableRequest;
import com.google.bigtable.admin.v2.InstanceName;
import com.google.bigtable.admin.v2.ListBackupsRequest;
import com.google.bigtable.admin.v2.ListBackupsResponse;
import com.google.bigtable.admin.v2.ListSnapshotsRequest;
import com.google.bigtable.admin.v2.ListSnapshotsResponse;
import com.google.bigtable.admin.v2.ListTablesRequest;
import com.google.bigtable.admin.v2.ListTablesResponse;
import com.google.bigtable.admin.v2.ModifyColumnFamiliesRequest;
import com.google.bigtable.admin.v2.RestoreTableRequest;
import com.google.bigtable.admin.v2.Snapshot;
import com.google.bigtable.admin.v2.SnapshotName;
import com.google.bigtable.admin.v2.SnapshotTableRequest;
import com.google.bigtable.admin.v2.Table;
import com.google.bigtable.admin.v2.TableName;
import com.google.bigtable.admin.v2.UpdateBackupRequest;
import com.google.common.collect.Lists;
import com.google.iam.v1.GetIamPolicyRequest;
import com.google.iam.v1.Policy;
import com.google.iam.v1.SetIamPolicyRequest;
import com.google.iam.v1.TestIamPermissionsRequest;
import com.google.iam.v1.TestIamPermissionsResponse;
import com.google.longrunning.Operation;
import com.google.protobuf.AbstractMessage;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import com.google.protobuf.FieldMask;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

@javax.annotation.Generated("by GAPIC")
public class BaseBigtableTableAdminClientTest {
  private static MockBigtableInstanceAdmin mockBigtableInstanceAdmin;
  private static MockBigtableTableAdmin mockBigtableTableAdmin;
  private static MockServiceHelper serviceHelper;
  private BaseBigtableTableAdminClient client;
  private LocalChannelProvider channelProvider;

  @BeforeClass
  public static void startStaticServer() {
    mockBigtableInstanceAdmin = new MockBigtableInstanceAdmin();
    mockBigtableTableAdmin = new MockBigtableTableAdmin();
    serviceHelper =
        new MockServiceHelper(
            UUID.randomUUID().toString(),
            Arrays.<MockGrpcService>asList(mockBigtableInstanceAdmin, mockBigtableTableAdmin));
    serviceHelper.start();
  }

  @AfterClass
  public static void stopServer() {
    serviceHelper.stop();
  }

  @Before
  public void setUp() throws IOException {
    serviceHelper.reset();
    channelProvider = serviceHelper.createChannelProvider();
    BaseBigtableTableAdminSettings settings =
        BaseBigtableTableAdminSettings.newBuilder()
            .setTransportChannelProvider(channelProvider)
            .setCredentialsProvider(NoCredentialsProvider.create())
            .build();
    client = BaseBigtableTableAdminClient.create(settings);
  }

  @After
  public void tearDown() throws Exception {
    client.close();
  }

  @Test
  @SuppressWarnings("all")
  public void createTableTest() {
    TableName name = TableName.of("[PROJECT]", "[INSTANCE]", "[TABLE]");
    Table expectedResponse = Table.newBuilder().setName(name.toString()).build();
    mockBigtableTableAdmin.addResponse(expectedResponse);

    InstanceName parent = InstanceName.of("[PROJECT]", "[INSTANCE]");
    String tableId = "tableId-895419604";
    Table table = Table.newBuilder().build();

    Table actualResponse = client.createTable(parent, tableId, table);
    Assert.assertEquals(expectedResponse, actualResponse);

    List<AbstractMessage> actualRequests = mockBigtableTableAdmin.getRequests();
    Assert.assertEquals(1, actualRequests.size());
    CreateTableRequest actualRequest = (CreateTableRequest) actualRequests.get(0);

    Assert.assertEquals(parent, InstanceName.parse(actualRequest.getParent()));
    Assert.assertEquals(tableId, actualRequest.getTableId());
    Assert.assertEquals(table, actualRequest.getTable());
    Assert.assertTrue(
        channelProvider.isHeaderSent(
            ApiClientHeaderProvider.getDefaultApiClientHeaderKey(),
            GaxGrpcProperties.getDefaultApiClientHeaderPattern()));
  }

  @Test
  @SuppressWarnings("all")
  public void createTableExceptionTest() throws Exception {
    StatusRuntimeException exception = new StatusRuntimeException(Status.INVALID_ARGUMENT);
    mockBigtableTableAdmin.addException(exception);

    try {
      InstanceName parent = InstanceName.of("[PROJECT]", "[INSTANCE]");
      String tableId = "tableId-895419604";
      Table table = Table.newBuilder().build();

      client.createTable(parent, tableId, table);
      Assert.fail("No exception raised");
    } catch (InvalidArgumentException e) {
      // Expected exception
    }
  }

  @Test
  @SuppressWarnings("all")
  public void createTableFromSnapshotTest() throws Exception {
    TableName name = TableName.of("[PROJECT]", "[INSTANCE]", "[TABLE]");
    Table expectedResponse = Table.newBuilder().setName(name.toString()).build();
    Operation resultOperation =
        Operation.newBuilder()
            .setName("createTableFromSnapshotTest")
            .setDone(true)
            .setResponse(Any.pack(expectedResponse))
            .build();
    mockBigtableTableAdmin.addResponse(resultOperation);

    InstanceName parent = InstanceName.of("[PROJECT]", "[INSTANCE]");
    String tableId = "tableId-895419604";
    SnapshotName sourceSnapshot =
        SnapshotName.of("[PROJECT]", "[INSTANCE]", "[CLUSTER]", "[SNAPSHOT]");

    Table actualResponse =
        client.createTableFromSnapshotAsync(parent, tableId, sourceSnapshot).get();
    Assert.assertEquals(expectedResponse, actualResponse);

    List<AbstractMessage> actualRequests = mockBigtableTableAdmin.getRequests();
    Assert.assertEquals(1, actualRequests.size());
    CreateTableFromSnapshotRequest actualRequest =
        (CreateTableFromSnapshotRequest) actualRequests.get(0);

    Assert.assertEquals(parent, InstanceName.parse(actualRequest.getParent()));
    Assert.assertEquals(tableId, actualRequest.getTableId());
    Assert.assertEquals(sourceSnapshot, SnapshotName.parse(actualRequest.getSourceSnapshot()));
    Assert.assertTrue(
        channelProvider.isHeaderSent(
            ApiClientHeaderProvider.getDefaultApiClientHeaderKey(),
            GaxGrpcProperties.getDefaultApiClientHeaderPattern()));
  }

  @Test
  @SuppressWarnings("all")
  public void createTableFromSnapshotExceptionTest() throws Exception {
    StatusRuntimeException exception = new StatusRuntimeException(Status.INVALID_ARGUMENT);
    mockBigtableTableAdmin.addException(exception);

    try {
      InstanceName parent = InstanceName.of("[PROJECT]", "[INSTANCE]");
      String tableId = "tableId-895419604";
      SnapshotName sourceSnapshot =
          SnapshotName.of("[PROJECT]", "[INSTANCE]", "[CLUSTER]", "[SNAPSHOT]");

      client.createTableFromSnapshotAsync(parent, tableId, sourceSnapshot).get();
      Assert.fail("No exception raised");
    } catch (ExecutionException e) {
      Assert.assertEquals(InvalidArgumentException.class, e.getCause().getClass());
      InvalidArgumentException apiException = (InvalidArgumentException) e.getCause();
      Assert.assertEquals(StatusCode.Code.INVALID_ARGUMENT, apiException.getStatusCode().getCode());
    }
  }

  @Test
  @SuppressWarnings("all")
  public void listTablesTest() {
    String nextPageToken = "";
    Table tablesElement = Table.newBuilder().build();
    List<Table> tables = Arrays.asList(tablesElement);
    ListTablesResponse expectedResponse =
        ListTablesResponse.newBuilder()
            .setNextPageToken(nextPageToken)
            .addAllTables(tables)
            .build();
    mockBigtableTableAdmin.addResponse(expectedResponse);

    InstanceName parent = InstanceName.of("[PROJECT]", "[INSTANCE]");

    ListTablesPagedResponse pagedListResponse = client.listTables(parent);

    List<Table> resources = Lists.newArrayList(pagedListResponse.iterateAll());
    Assert.assertEquals(1, resources.size());
    Assert.assertEquals(expectedResponse.getTablesList().get(0), resources.get(0));

    List<AbstractMessage> actualRequests = mockBigtableTableAdmin.getRequests();
    Assert.assertEquals(1, actualRequests.size());
    ListTablesRequest actualRequest = (ListTablesRequest) actualRequests.get(0);

    Assert.assertEquals(parent, InstanceName.parse(actualRequest.getParent()));
    Assert.assertTrue(
        channelProvider.isHeaderSent(
            ApiClientHeaderProvider.getDefaultApiClientHeaderKey(),
            GaxGrpcProperties.getDefaultApiClientHeaderPattern()));
  }

  @Test
  @SuppressWarnings("all")
  public void listTablesExceptionTest() throws Exception {
    StatusRuntimeException exception = new StatusRuntimeException(Status.INVALID_ARGUMENT);
    mockBigtableTableAdmin.addException(exception);

    try {
      InstanceName parent = InstanceName.of("[PROJECT]", "[INSTANCE]");

      client.listTables(parent);
      Assert.fail("No exception raised");
    } catch (InvalidArgumentException e) {
      // Expected exception
    }
  }

  @Test
  @SuppressWarnings("all")
  public void getTableTest() {
    TableName name2 = TableName.of("[PROJECT]", "[INSTANCE]", "[TABLE]");
    Table expectedResponse = Table.newBuilder().setName(name2.toString()).build();
    mockBigtableTableAdmin.addResponse(expectedResponse);

    TableName name = TableName.of("[PROJECT]", "[INSTANCE]", "[TABLE]");

    Table actualResponse = client.getTable(name);
    Assert.assertEquals(expectedResponse, actualResponse);

    List<AbstractMessage> actualRequests = mockBigtableTableAdmin.getRequests();
    Assert.assertEquals(1, actualRequests.size());
    GetTableRequest actualRequest = (GetTableRequest) actualRequests.get(0);

    Assert.assertEquals(name, TableName.parse(actualRequest.getName()));
    Assert.assertTrue(
        channelProvider.isHeaderSent(
            ApiClientHeaderProvider.getDefaultApiClientHeaderKey(),
            GaxGrpcProperties.getDefaultApiClientHeaderPattern()));
  }

  @Test
  @SuppressWarnings("all")
  public void getTableExceptionTest() throws Exception {
    StatusRuntimeException exception = new StatusRuntimeException(Status.INVALID_ARGUMENT);
    mockBigtableTableAdmin.addException(exception);

    try {
      TableName name = TableName.of("[PROJECT]", "[INSTANCE]", "[TABLE]");

      client.getTable(name);
      Assert.fail("No exception raised");
    } catch (InvalidArgumentException e) {
      // Expected exception
    }
  }

  @Test
  @SuppressWarnings("all")
  public void deleteTableTest() {
    Empty expectedResponse = Empty.newBuilder().build();
    mockBigtableTableAdmin.addResponse(expectedResponse);

    TableName name = TableName.of("[PROJECT]", "[INSTANCE]", "[TABLE]");

    client.deleteTable(name);

    List<AbstractMessage> actualRequests = mockBigtableTableAdmin.getRequests();
    Assert.assertEquals(1, actualRequests.size());
    DeleteTableRequest actualRequest = (DeleteTableRequest) actualRequests.get(0);

    Assert.assertEquals(name, TableName.parse(actualRequest.getName()));
    Assert.assertTrue(
        channelProvider.isHeaderSent(
            ApiClientHeaderProvider.getDefaultApiClientHeaderKey(),
            GaxGrpcProperties.getDefaultApiClientHeaderPattern()));
  }

  @Test
  @SuppressWarnings("all")
  public void deleteTableExceptionTest() throws Exception {
    StatusRuntimeException exception = new StatusRuntimeException(Status.INVALID_ARGUMENT);
    mockBigtableTableAdmin.addException(exception);

    try {
      TableName name = TableName.of("[PROJECT]", "[INSTANCE]", "[TABLE]");

      client.deleteTable(name);
      Assert.fail("No exception raised");
    } catch (InvalidArgumentException e) {
      // Expected exception
    }
  }

  @Test
  @SuppressWarnings("all")
  public void modifyColumnFamiliesTest() {
    TableName name2 = TableName.of("[PROJECT]", "[INSTANCE]", "[TABLE]");
    Table expectedResponse = Table.newBuilder().setName(name2.toString()).build();
    mockBigtableTableAdmin.addResponse(expectedResponse);

    TableName name = TableName.of("[PROJECT]", "[INSTANCE]", "[TABLE]");
    List<ModifyColumnFamiliesRequest.Modification> modifications = new ArrayList<>();

    Table actualResponse = client.modifyColumnFamilies(name, modifications);
    Assert.assertEquals(expectedResponse, actualResponse);

    List<AbstractMessage> actualRequests = mockBigtableTableAdmin.getRequests();
    Assert.assertEquals(1, actualRequests.size());
    ModifyColumnFamiliesRequest actualRequest = (ModifyColumnFamiliesRequest) actualRequests.get(0);

    Assert.assertEquals(name, TableName.parse(actualRequest.getName()));
    Assert.assertEquals(modifications, actualRequest.getModificationsList());
    Assert.assertTrue(
        channelProvider.isHeaderSent(
            ApiClientHeaderProvider.getDefaultApiClientHeaderKey(),
            GaxGrpcProperties.getDefaultApiClientHeaderPattern()));
  }

  @Test
  @SuppressWarnings("all")
  public void modifyColumnFamiliesExceptionTest() throws Exception {
    StatusRuntimeException exception = new StatusRuntimeException(Status.INVALID_ARGUMENT);
    mockBigtableTableAdmin.addException(exception);

    try {
      TableName name = TableName.of("[PROJECT]", "[INSTANCE]", "[TABLE]");
      List<ModifyColumnFamiliesRequest.Modification> modifications = new ArrayList<>();

      client.modifyColumnFamilies(name, modifications);
      Assert.fail("No exception raised");
    } catch (InvalidArgumentException e) {
      // Expected exception
    }
  }

  @Test
  @SuppressWarnings("all")
  public void dropRowRangeTest() {
    Empty expectedResponse = Empty.newBuilder().build();
    mockBigtableTableAdmin.addResponse(expectedResponse);

    TableName name = TableName.of("[PROJECT]", "[INSTANCE]", "[TABLE]");
    DropRowRangeRequest request = DropRowRangeRequest.newBuilder().setName(name.toString()).build();

    client.dropRowRange(request);

    List<AbstractMessage> actualRequests = mockBigtableTableAdmin.getRequests();
    Assert.assertEquals(1, actualRequests.size());
    DropRowRangeRequest actualRequest = (DropRowRangeRequest) actualRequests.get(0);

    Assert.assertEquals(name, TableName.parse(actualRequest.getName()));
    Assert.assertTrue(
        channelProvider.isHeaderSent(
            ApiClientHeaderProvider.getDefaultApiClientHeaderKey(),
            GaxGrpcProperties.getDefaultApiClientHeaderPattern()));
  }

  @Test
  @SuppressWarnings("all")
  public void dropRowRangeExceptionTest() throws Exception {
    StatusRuntimeException exception = new StatusRuntimeException(Status.INVALID_ARGUMENT);
    mockBigtableTableAdmin.addException(exception);

    try {
      TableName name = TableName.of("[PROJECT]", "[INSTANCE]", "[TABLE]");
      DropRowRangeRequest request =
          DropRowRangeRequest.newBuilder().setName(name.toString()).build();

      client.dropRowRange(request);
      Assert.fail("No exception raised");
    } catch (InvalidArgumentException e) {
      // Expected exception
    }
  }

  @Test
  @SuppressWarnings("all")
  public void generateConsistencyTokenTest() {
    String consistencyToken = "consistencyToken-1090516718";
    GenerateConsistencyTokenResponse expectedResponse =
        GenerateConsistencyTokenResponse.newBuilder().setConsistencyToken(consistencyToken).build();
    mockBigtableTableAdmin.addResponse(expectedResponse);

    TableName name = TableName.of("[PROJECT]", "[INSTANCE]", "[TABLE]");

    GenerateConsistencyTokenResponse actualResponse = client.generateConsistencyToken(name);
    Assert.assertEquals(expectedResponse, actualResponse);

    List<AbstractMessage> actualRequests = mockBigtableTableAdmin.getRequests();
    Assert.assertEquals(1, actualRequests.size());
    GenerateConsistencyTokenRequest actualRequest =
        (GenerateConsistencyTokenRequest) actualRequests.get(0);

    Assert.assertEquals(name, TableName.parse(actualRequest.getName()));
    Assert.assertTrue(
        channelProvider.isHeaderSent(
            ApiClientHeaderProvider.getDefaultApiClientHeaderKey(),
            GaxGrpcProperties.getDefaultApiClientHeaderPattern()));
  }

  @Test
  @SuppressWarnings("all")
  public void generateConsistencyTokenExceptionTest() throws Exception {
    StatusRuntimeException exception = new StatusRuntimeException(Status.INVALID_ARGUMENT);
    mockBigtableTableAdmin.addException(exception);

    try {
      TableName name = TableName.of("[PROJECT]", "[INSTANCE]", "[TABLE]");

      client.generateConsistencyToken(name);
      Assert.fail("No exception raised");
    } catch (InvalidArgumentException e) {
      // Expected exception
    }
  }

  @Test
  @SuppressWarnings("all")
  public void checkConsistencyTest() {
    boolean consistent = true;
    CheckConsistencyResponse expectedResponse =
        CheckConsistencyResponse.newBuilder().setConsistent(consistent).build();
    mockBigtableTableAdmin.addResponse(expectedResponse);

    TableName name = TableName.of("[PROJECT]", "[INSTANCE]", "[TABLE]");
    String consistencyToken = "consistencyToken-1090516718";

    CheckConsistencyResponse actualResponse = client.checkConsistency(name, consistencyToken);
    Assert.assertEquals(expectedResponse, actualResponse);

    List<AbstractMessage> actualRequests = mockBigtableTableAdmin.getRequests();
    Assert.assertEquals(1, actualRequests.size());
    CheckConsistencyRequest actualRequest = (CheckConsistencyRequest) actualRequests.get(0);

    Assert.assertEquals(name, TableName.parse(actualRequest.getName()));
    Assert.assertEquals(consistencyToken, actualRequest.getConsistencyToken());
    Assert.assertTrue(
        channelProvider.isHeaderSent(
            ApiClientHeaderProvider.getDefaultApiClientHeaderKey(),
            GaxGrpcProperties.getDefaultApiClientHeaderPattern()));
  }

  @Test
  @SuppressWarnings("all")
  public void checkConsistencyExceptionTest() throws Exception {
    StatusRuntimeException exception = new StatusRuntimeException(Status.INVALID_ARGUMENT);
    mockBigtableTableAdmin.addException(exception);

    try {
      TableName name = TableName.of("[PROJECT]", "[INSTANCE]", "[TABLE]");
      String consistencyToken = "consistencyToken-1090516718";

      client.checkConsistency(name, consistencyToken);
      Assert.fail("No exception raised");
    } catch (InvalidArgumentException e) {
      // Expected exception
    }
  }

  @Test
  @SuppressWarnings("all")
  public void getIamPolicyTest() {
    int version = 351608024;
    ByteString etag = ByteString.copyFromUtf8("etag3123477");
    Policy expectedResponse = Policy.newBuilder().setVersion(version).setEtag(etag).build();
    mockBigtableTableAdmin.addResponse(expectedResponse);

    ResourceName resource = AppProfileName.of("[PROJECT]", "[INSTANCE]", "[APP_PROFILE]");

    Policy actualResponse = client.getIamPolicy(resource);
    Assert.assertEquals(expectedResponse, actualResponse);

    List<AbstractMessage> actualRequests = mockBigtableTableAdmin.getRequests();
    Assert.assertEquals(1, actualRequests.size());
    GetIamPolicyRequest actualRequest = (GetIamPolicyRequest) actualRequests.get(0);

    Assert.assertEquals(Objects.toString(resource), Objects.toString(actualRequest.getResource()));
    Assert.assertTrue(
        channelProvider.isHeaderSent(
            ApiClientHeaderProvider.getDefaultApiClientHeaderKey(),
            GaxGrpcProperties.getDefaultApiClientHeaderPattern()));
  }

  @Test
  @SuppressWarnings("all")
  public void getIamPolicyExceptionTest() throws Exception {
    StatusRuntimeException exception = new StatusRuntimeException(Status.INVALID_ARGUMENT);
    mockBigtableTableAdmin.addException(exception);

    try {
      ResourceName resource = AppProfileName.of("[PROJECT]", "[INSTANCE]", "[APP_PROFILE]");

      client.getIamPolicy(resource);
      Assert.fail("No exception raised");
    } catch (InvalidArgumentException e) {
      // Expected exception
    }
  }

  @Test
  @SuppressWarnings("all")
  public void setIamPolicyTest() {
    int version = 351608024;
    ByteString etag = ByteString.copyFromUtf8("etag3123477");
    Policy expectedResponse = Policy.newBuilder().setVersion(version).setEtag(etag).build();
    mockBigtableTableAdmin.addResponse(expectedResponse);

    ResourceName resource = AppProfileName.of("[PROJECT]", "[INSTANCE]", "[APP_PROFILE]");
    Policy policy = Policy.newBuilder().build();

    Policy actualResponse = client.setIamPolicy(resource, policy);
    Assert.assertEquals(expectedResponse, actualResponse);

    List<AbstractMessage> actualRequests = mockBigtableTableAdmin.getRequests();
    Assert.assertEquals(1, actualRequests.size());
    SetIamPolicyRequest actualRequest = (SetIamPolicyRequest) actualRequests.get(0);

    Assert.assertEquals(Objects.toString(resource), Objects.toString(actualRequest.getResource()));
    Assert.assertEquals(policy, actualRequest.getPolicy());
    Assert.assertTrue(
        channelProvider.isHeaderSent(
            ApiClientHeaderProvider.getDefaultApiClientHeaderKey(),
            GaxGrpcProperties.getDefaultApiClientHeaderPattern()));
  }

  @Test
  @SuppressWarnings("all")
  public void setIamPolicyExceptionTest() throws Exception {
    StatusRuntimeException exception = new StatusRuntimeException(Status.INVALID_ARGUMENT);
    mockBigtableTableAdmin.addException(exception);

    try {
      ResourceName resource = AppProfileName.of("[PROJECT]", "[INSTANCE]", "[APP_PROFILE]");
      Policy policy = Policy.newBuilder().build();

      client.setIamPolicy(resource, policy);
      Assert.fail("No exception raised");
    } catch (InvalidArgumentException e) {
      // Expected exception
    }
  }

  @Test
  @SuppressWarnings("all")
  public void testIamPermissionsTest() {
    TestIamPermissionsResponse expectedResponse = TestIamPermissionsResponse.newBuilder().build();
    mockBigtableTableAdmin.addResponse(expectedResponse);

    ResourceName resource = AppProfileName.of("[PROJECT]", "[INSTANCE]", "[APP_PROFILE]");
    List<String> permissions = new ArrayList<>();

    TestIamPermissionsResponse actualResponse = client.testIamPermissions(resource, permissions);
    Assert.assertEquals(expectedResponse, actualResponse);

    List<AbstractMessage> actualRequests = mockBigtableTableAdmin.getRequests();
    Assert.assertEquals(1, actualRequests.size());
    TestIamPermissionsRequest actualRequest = (TestIamPermissionsRequest) actualRequests.get(0);

    Assert.assertEquals(Objects.toString(resource), Objects.toString(actualRequest.getResource()));
    Assert.assertEquals(permissions, actualRequest.getPermissionsList());
    Assert.assertTrue(
        channelProvider.isHeaderSent(
            ApiClientHeaderProvider.getDefaultApiClientHeaderKey(),
            GaxGrpcProperties.getDefaultApiClientHeaderPattern()));
  }

  @Test
  @SuppressWarnings("all")
  public void testIamPermissionsExceptionTest() throws Exception {
    StatusRuntimeException exception = new StatusRuntimeException(Status.INVALID_ARGUMENT);
    mockBigtableTableAdmin.addException(exception);

    try {
      ResourceName resource = AppProfileName.of("[PROJECT]", "[INSTANCE]", "[APP_PROFILE]");
      List<String> permissions = new ArrayList<>();

      client.testIamPermissions(resource, permissions);
      Assert.fail("No exception raised");
    } catch (InvalidArgumentException e) {
      // Expected exception
    }
  }

  @Test
  @SuppressWarnings("all")
  public void snapshotTableTest() throws Exception {
    SnapshotName name2 = SnapshotName.of("[PROJECT]", "[INSTANCE]", "[CLUSTER]", "[SNAPSHOT]");
    long dataSizeBytes = 2110122398L;
    String description2 = "description2568623279";
    Snapshot expectedResponse =
        Snapshot.newBuilder()
            .setName(name2.toString())
            .setDataSizeBytes(dataSizeBytes)
            .setDescription(description2)
            .build();
    Operation resultOperation =
        Operation.newBuilder()
            .setName("snapshotTableTest")
            .setDone(true)
            .setResponse(Any.pack(expectedResponse))
            .build();
    mockBigtableTableAdmin.addResponse(resultOperation);

    TableName name = TableName.of("[PROJECT]", "[INSTANCE]", "[TABLE]");
    ClusterName cluster = ClusterName.of("[PROJECT]", "[INSTANCE]", "[CLUSTER]");
    String snapshotId = "snapshotId-168585866";
    String description = "description-1724546052";

    Snapshot actualResponse =
        client.snapshotTableAsync(name, cluster, snapshotId, description).get();
    Assert.assertEquals(expectedResponse, actualResponse);

    List<AbstractMessage> actualRequests = mockBigtableTableAdmin.getRequests();
    Assert.assertEquals(1, actualRequests.size());
    SnapshotTableRequest actualRequest = (SnapshotTableRequest) actualRequests.get(0);

    Assert.assertEquals(name, TableName.parse(actualRequest.getName()));
    Assert.assertEquals(cluster, ClusterName.parse(actualRequest.getCluster()));
    Assert.assertEquals(snapshotId, actualRequest.getSnapshotId());
    Assert.assertEquals(description, actualRequest.getDescription());
    Assert.assertTrue(
        channelProvider.isHeaderSent(
            ApiClientHeaderProvider.getDefaultApiClientHeaderKey(),
            GaxGrpcProperties.getDefaultApiClientHeaderPattern()));
  }

  @Test
  @SuppressWarnings("all")
  public void snapshotTableExceptionTest() throws Exception {
    StatusRuntimeException exception = new StatusRuntimeException(Status.INVALID_ARGUMENT);
    mockBigtableTableAdmin.addException(exception);

    try {
      TableName name = TableName.of("[PROJECT]", "[INSTANCE]", "[TABLE]");
      ClusterName cluster = ClusterName.of("[PROJECT]", "[INSTANCE]", "[CLUSTER]");
      String snapshotId = "snapshotId-168585866";
      String description = "description-1724546052";

      client.snapshotTableAsync(name, cluster, snapshotId, description).get();
      Assert.fail("No exception raised");
    } catch (ExecutionException e) {
      Assert.assertEquals(InvalidArgumentException.class, e.getCause().getClass());
      InvalidArgumentException apiException = (InvalidArgumentException) e.getCause();
      Assert.assertEquals(StatusCode.Code.INVALID_ARGUMENT, apiException.getStatusCode().getCode());
    }
  }

  @Test
  @SuppressWarnings("all")
  public void getSnapshotTest() {
    SnapshotName name2 = SnapshotName.of("[PROJECT]", "[INSTANCE]", "[CLUSTER]", "[SNAPSHOT]");
    long dataSizeBytes = 2110122398L;
    String description = "description-1724546052";
    Snapshot expectedResponse =
        Snapshot.newBuilder()
            .setName(name2.toString())
            .setDataSizeBytes(dataSizeBytes)
            .setDescription(description)
            .build();
    mockBigtableTableAdmin.addResponse(expectedResponse);

    SnapshotName name = SnapshotName.of("[PROJECT]", "[INSTANCE]", "[CLUSTER]", "[SNAPSHOT]");

    Snapshot actualResponse = client.getSnapshot(name);
    Assert.assertEquals(expectedResponse, actualResponse);

    List<AbstractMessage> actualRequests = mockBigtableTableAdmin.getRequests();
    Assert.assertEquals(1, actualRequests.size());
    GetSnapshotRequest actualRequest = (GetSnapshotRequest) actualRequests.get(0);

    Assert.assertEquals(name, SnapshotName.parse(actualRequest.getName()));
    Assert.assertTrue(
        channelProvider.isHeaderSent(
            ApiClientHeaderProvider.getDefaultApiClientHeaderKey(),
            GaxGrpcProperties.getDefaultApiClientHeaderPattern()));
  }

  @Test
  @SuppressWarnings("all")
  public void getSnapshotExceptionTest() throws Exception {
    StatusRuntimeException exception = new StatusRuntimeException(Status.INVALID_ARGUMENT);
    mockBigtableTableAdmin.addException(exception);

    try {
      SnapshotName name = SnapshotName.of("[PROJECT]", "[INSTANCE]", "[CLUSTER]", "[SNAPSHOT]");

      client.getSnapshot(name);
      Assert.fail("No exception raised");
    } catch (InvalidArgumentException e) {
      // Expected exception
    }
  }

  @Test
  @SuppressWarnings("all")
  public void listSnapshotsTest() {
    String nextPageToken = "";
    Snapshot snapshotsElement = Snapshot.newBuilder().build();
    List<Snapshot> snapshots = Arrays.asList(snapshotsElement);
    ListSnapshotsResponse expectedResponse =
        ListSnapshotsResponse.newBuilder()
            .setNextPageToken(nextPageToken)
            .addAllSnapshots(snapshots)
            .build();
    mockBigtableTableAdmin.addResponse(expectedResponse);

    ClusterName parent = ClusterName.of("[PROJECT]", "[INSTANCE]", "[CLUSTER]");

    ListSnapshotsPagedResponse pagedListResponse = client.listSnapshots(parent);

    List<Snapshot> resources = Lists.newArrayList(pagedListResponse.iterateAll());
    Assert.assertEquals(1, resources.size());
    Assert.assertEquals(expectedResponse.getSnapshotsList().get(0), resources.get(0));

    List<AbstractMessage> actualRequests = mockBigtableTableAdmin.getRequests();
    Assert.assertEquals(1, actualRequests.size());
    ListSnapshotsRequest actualRequest = (ListSnapshotsRequest) actualRequests.get(0);

    Assert.assertEquals(parent, ClusterName.parse(actualRequest.getParent()));
    Assert.assertTrue(
        channelProvider.isHeaderSent(
            ApiClientHeaderProvider.getDefaultApiClientHeaderKey(),
            GaxGrpcProperties.getDefaultApiClientHeaderPattern()));
  }

  @Test
  @SuppressWarnings("all")
  public void listSnapshotsExceptionTest() throws Exception {
    StatusRuntimeException exception = new StatusRuntimeException(Status.INVALID_ARGUMENT);
    mockBigtableTableAdmin.addException(exception);

    try {
      ClusterName parent = ClusterName.of("[PROJECT]", "[INSTANCE]", "[CLUSTER]");

      client.listSnapshots(parent);
      Assert.fail("No exception raised");
    } catch (InvalidArgumentException e) {
      // Expected exception
    }
  }

  @Test
  @SuppressWarnings("all")
  public void deleteSnapshotTest() {
    Empty expectedResponse = Empty.newBuilder().build();
    mockBigtableTableAdmin.addResponse(expectedResponse);

    SnapshotName name = SnapshotName.of("[PROJECT]", "[INSTANCE]", "[CLUSTER]", "[SNAPSHOT]");

    client.deleteSnapshot(name);

    List<AbstractMessage> actualRequests = mockBigtableTableAdmin.getRequests();
    Assert.assertEquals(1, actualRequests.size());
    DeleteSnapshotRequest actualRequest = (DeleteSnapshotRequest) actualRequests.get(0);

    Assert.assertEquals(name, SnapshotName.parse(actualRequest.getName()));
    Assert.assertTrue(
        channelProvider.isHeaderSent(
            ApiClientHeaderProvider.getDefaultApiClientHeaderKey(),
            GaxGrpcProperties.getDefaultApiClientHeaderPattern()));
  }

  @Test
  @SuppressWarnings("all")
  public void deleteSnapshotExceptionTest() throws Exception {
    StatusRuntimeException exception = new StatusRuntimeException(Status.INVALID_ARGUMENT);
    mockBigtableTableAdmin.addException(exception);

    try {
      SnapshotName name = SnapshotName.of("[PROJECT]", "[INSTANCE]", "[CLUSTER]", "[SNAPSHOT]");

      client.deleteSnapshot(name);
      Assert.fail("No exception raised");
    } catch (InvalidArgumentException e) {
      // Expected exception
    }
  }

  @Test
  @SuppressWarnings("all")
  public void createBackupTest() throws Exception {
    BackupName name = BackupName.of("[PROJECT]", "[INSTANCE]", "[CLUSTER]", "[BACKUP]");
    String sourceTable = "sourceTable1670858410";
    long sizeBytes = 1796325715L;
    Backup expectedResponse =
        Backup.newBuilder()
            .setName(name.toString())
            .setSourceTable(sourceTable)
            .setSizeBytes(sizeBytes)
            .build();
    Operation resultOperation =
        Operation.newBuilder()
            .setName("createBackupTest")
            .setDone(true)
            .setResponse(Any.pack(expectedResponse))
            .build();
    mockBigtableTableAdmin.addResponse(resultOperation);

    ClusterName parent = ClusterName.of("[PROJECT]", "[INSTANCE]", "[CLUSTER]");
    String backupId = "backupId1355353272";
    Backup backup = Backup.newBuilder().build();

    Backup actualResponse = client.createBackupAsync(parent, backupId, backup).get();
    Assert.assertEquals(expectedResponse, actualResponse);

    List<AbstractMessage> actualRequests = mockBigtableTableAdmin.getRequests();
    Assert.assertEquals(1, actualRequests.size());
    CreateBackupRequest actualRequest = (CreateBackupRequest) actualRequests.get(0);

    Assert.assertEquals(parent, ClusterName.parse(actualRequest.getParent()));
    Assert.assertEquals(backupId, actualRequest.getBackupId());
    Assert.assertEquals(backup, actualRequest.getBackup());
    Assert.assertTrue(
        channelProvider.isHeaderSent(
            ApiClientHeaderProvider.getDefaultApiClientHeaderKey(),
            GaxGrpcProperties.getDefaultApiClientHeaderPattern()));
  }

  @Test
  @SuppressWarnings("all")
  public void createBackupExceptionTest() throws Exception {
    StatusRuntimeException exception = new StatusRuntimeException(Status.INVALID_ARGUMENT);
    mockBigtableTableAdmin.addException(exception);

    try {
      ClusterName parent = ClusterName.of("[PROJECT]", "[INSTANCE]", "[CLUSTER]");
      String backupId = "backupId1355353272";
      Backup backup = Backup.newBuilder().build();

      client.createBackupAsync(parent, backupId, backup).get();
      Assert.fail("No exception raised");
    } catch (ExecutionException e) {
      Assert.assertEquals(InvalidArgumentException.class, e.getCause().getClass());
      InvalidArgumentException apiException = (InvalidArgumentException) e.getCause();
      Assert.assertEquals(StatusCode.Code.INVALID_ARGUMENT, apiException.getStatusCode().getCode());
    }
  }

  @Test
  @SuppressWarnings("all")
  public void getBackupTest() {
    BackupName name2 = BackupName.of("[PROJECT]", "[INSTANCE]", "[CLUSTER]", "[BACKUP]");
    String sourceTable = "sourceTable1670858410";
    long sizeBytes = 1796325715L;
    Backup expectedResponse =
        Backup.newBuilder()
            .setName(name2.toString())
            .setSourceTable(sourceTable)
            .setSizeBytes(sizeBytes)
            .build();
    mockBigtableTableAdmin.addResponse(expectedResponse);

    BackupName name = BackupName.of("[PROJECT]", "[INSTANCE]", "[CLUSTER]", "[BACKUP]");

    Backup actualResponse = client.getBackup(name);
    Assert.assertEquals(expectedResponse, actualResponse);

    List<AbstractMessage> actualRequests = mockBigtableTableAdmin.getRequests();
    Assert.assertEquals(1, actualRequests.size());
    GetBackupRequest actualRequest = (GetBackupRequest) actualRequests.get(0);

    Assert.assertEquals(name, BackupName.parse(actualRequest.getName()));
    Assert.assertTrue(
        channelProvider.isHeaderSent(
            ApiClientHeaderProvider.getDefaultApiClientHeaderKey(),
            GaxGrpcProperties.getDefaultApiClientHeaderPattern()));
  }

  @Test
  @SuppressWarnings("all")
  public void getBackupExceptionTest() throws Exception {
    StatusRuntimeException exception = new StatusRuntimeException(Status.INVALID_ARGUMENT);
    mockBigtableTableAdmin.addException(exception);

    try {
      BackupName name = BackupName.of("[PROJECT]", "[INSTANCE]", "[CLUSTER]", "[BACKUP]");

      client.getBackup(name);
      Assert.fail("No exception raised");
    } catch (InvalidArgumentException e) {
      // Expected exception
    }
  }

  @Test
  @SuppressWarnings("all")
  public void listBackupsTest() {
    String nextPageToken = "";
    Backup backupsElement = Backup.newBuilder().build();
    List<Backup> backups = Arrays.asList(backupsElement);
    ListBackupsResponse expectedResponse =
        ListBackupsResponse.newBuilder()
            .setNextPageToken(nextPageToken)
            .addAllBackups(backups)
            .build();
    mockBigtableTableAdmin.addResponse(expectedResponse);

    ClusterName parent = ClusterName.of("[PROJECT]", "[INSTANCE]", "[CLUSTER]");

    ListBackupsPagedResponse pagedListResponse = client.listBackups(parent);

    List<Backup> resources = Lists.newArrayList(pagedListResponse.iterateAll());
    Assert.assertEquals(1, resources.size());
    Assert.assertEquals(expectedResponse.getBackupsList().get(0), resources.get(0));

    List<AbstractMessage> actualRequests = mockBigtableTableAdmin.getRequests();
    Assert.assertEquals(1, actualRequests.size());
    ListBackupsRequest actualRequest = (ListBackupsRequest) actualRequests.get(0);

    Assert.assertEquals(parent, ClusterName.parse(actualRequest.getParent()));
    Assert.assertTrue(
        channelProvider.isHeaderSent(
            ApiClientHeaderProvider.getDefaultApiClientHeaderKey(),
            GaxGrpcProperties.getDefaultApiClientHeaderPattern()));
  }

  @Test
  @SuppressWarnings("all")
  public void listBackupsExceptionTest() throws Exception {
    StatusRuntimeException exception = new StatusRuntimeException(Status.INVALID_ARGUMENT);
    mockBigtableTableAdmin.addException(exception);

    try {
      ClusterName parent = ClusterName.of("[PROJECT]", "[INSTANCE]", "[CLUSTER]");

      client.listBackups(parent);
      Assert.fail("No exception raised");
    } catch (InvalidArgumentException e) {
      // Expected exception
    }
  }

  @Test
  @SuppressWarnings("all")
  public void updateBackupTest() {
    BackupName name = BackupName.of("[PROJECT]", "[INSTANCE]", "[CLUSTER]", "[BACKUP]");
    String sourceTable = "sourceTable1670858410";
    long sizeBytes = 1796325715L;
    Backup expectedResponse =
        Backup.newBuilder()
            .setName(name.toString())
            .setSourceTable(sourceTable)
            .setSizeBytes(sizeBytes)
            .build();
    mockBigtableTableAdmin.addResponse(expectedResponse);

    Backup backup = Backup.newBuilder().build();
    FieldMask updateMask = FieldMask.newBuilder().build();

    Backup actualResponse = client.updateBackup(backup, updateMask);
    Assert.assertEquals(expectedResponse, actualResponse);

    List<AbstractMessage> actualRequests = mockBigtableTableAdmin.getRequests();
    Assert.assertEquals(1, actualRequests.size());
    UpdateBackupRequest actualRequest = (UpdateBackupRequest) actualRequests.get(0);

    Assert.assertEquals(backup, actualRequest.getBackup());
    Assert.assertEquals(updateMask, actualRequest.getUpdateMask());
    Assert.assertTrue(
        channelProvider.isHeaderSent(
            ApiClientHeaderProvider.getDefaultApiClientHeaderKey(),
            GaxGrpcProperties.getDefaultApiClientHeaderPattern()));
  }

  @Test
  @SuppressWarnings("all")
  public void updateBackupExceptionTest() throws Exception {
    StatusRuntimeException exception = new StatusRuntimeException(Status.INVALID_ARGUMENT);
    mockBigtableTableAdmin.addException(exception);

    try {
      Backup backup = Backup.newBuilder().build();
      FieldMask updateMask = FieldMask.newBuilder().build();

      client.updateBackup(backup, updateMask);
      Assert.fail("No exception raised");
    } catch (InvalidArgumentException e) {
      // Expected exception
    }
  }

  @Test
  @SuppressWarnings("all")
  public void deleteBackupTest() {
    Empty expectedResponse = Empty.newBuilder().build();
    mockBigtableTableAdmin.addResponse(expectedResponse);

    BackupName name = BackupName.of("[PROJECT]", "[INSTANCE]", "[CLUSTER]", "[BACKUP]");

    client.deleteBackup(name);

    List<AbstractMessage> actualRequests = mockBigtableTableAdmin.getRequests();
    Assert.assertEquals(1, actualRequests.size());
    DeleteBackupRequest actualRequest = (DeleteBackupRequest) actualRequests.get(0);

    Assert.assertEquals(name, BackupName.parse(actualRequest.getName()));
    Assert.assertTrue(
        channelProvider.isHeaderSent(
            ApiClientHeaderProvider.getDefaultApiClientHeaderKey(),
            GaxGrpcProperties.getDefaultApiClientHeaderPattern()));
  }

  @Test
  @SuppressWarnings("all")
  public void deleteBackupExceptionTest() throws Exception {
    StatusRuntimeException exception = new StatusRuntimeException(Status.INVALID_ARGUMENT);
    mockBigtableTableAdmin.addException(exception);

    try {
      BackupName name = BackupName.of("[PROJECT]", "[INSTANCE]", "[CLUSTER]", "[BACKUP]");

      client.deleteBackup(name);
      Assert.fail("No exception raised");
    } catch (InvalidArgumentException e) {
      // Expected exception
    }
  }

  @Test
  @SuppressWarnings("all")
  public void restoreTableTest() throws Exception {
    TableName name = TableName.of("[PROJECT]", "[INSTANCE]", "[TABLE]");
    Table expectedResponse = Table.newBuilder().setName(name.toString()).build();
    Operation resultOperation =
        Operation.newBuilder()
            .setName("restoreTableTest")
            .setDone(true)
            .setResponse(Any.pack(expectedResponse))
            .build();
    mockBigtableTableAdmin.addResponse(resultOperation);

    RestoreTableRequest request = RestoreTableRequest.newBuilder().build();

    Table actualResponse = client.restoreTableAsync(request).get();
    Assert.assertEquals(expectedResponse, actualResponse);

    List<AbstractMessage> actualRequests = mockBigtableTableAdmin.getRequests();
    Assert.assertEquals(1, actualRequests.size());
    RestoreTableRequest actualRequest = (RestoreTableRequest) actualRequests.get(0);

    Assert.assertTrue(
        channelProvider.isHeaderSent(
            ApiClientHeaderProvider.getDefaultApiClientHeaderKey(),
            GaxGrpcProperties.getDefaultApiClientHeaderPattern()));
  }

  @Test
  @SuppressWarnings("all")
  public void restoreTableExceptionTest() throws Exception {
    StatusRuntimeException exception = new StatusRuntimeException(Status.INVALID_ARGUMENT);
    mockBigtableTableAdmin.addException(exception);

    try {
      RestoreTableRequest request = RestoreTableRequest.newBuilder().build();

      client.restoreTableAsync(request).get();
      Assert.fail("No exception raised");
    } catch (ExecutionException e) {
      Assert.assertEquals(InvalidArgumentException.class, e.getCause().getClass());
      InvalidArgumentException apiException = (InvalidArgumentException) e.getCause();
      Assert.assertEquals(StatusCode.Code.INVALID_ARGUMENT, apiException.getStatusCode().getCode());
    }
  }
}
