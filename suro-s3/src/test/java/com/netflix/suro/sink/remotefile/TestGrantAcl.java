package com.netflix.suro.sink.remotefile;

import org.jets3t.service.acl.AccessControlList;
import org.jets3t.service.acl.CanonicalGrantee;
import org.jets3t.service.acl.GrantAndPermission;
import org.jets3t.service.acl.Permission;
import org.jets3t.service.acl.gs.GSAccessControlList;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.model.S3Object;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;

public class TestGrantAcl {
    @Test
    public void test() throws Exception {
        RestS3Service s3Service = mock(RestS3Service.class);
        AccessControlList acl = new AccessControlList();
        doReturn(acl).when(s3Service).getObjectAcl("bucket", "key");
        doNothing().when(s3Service).putObjectAcl("bucket", "key", acl);

        GrantAcl grantAcl = new GrantAcl(s3Service, "1,2,3", 1);
        S3Object obj = new S3Object("key");
        obj.setBucketName("bucket");
        obj.setAcl(GSAccessControlList.REST_CANNED_BUCKET_OWNER_FULL_CONTROL);
        assertTrue(grantAcl.grantAcl(obj));

        Set<GrantAndPermission> grants = new HashSet<GrantAndPermission>(Arrays.asList(acl.getGrantAndPermissions()));
        assertEquals(grants.size(), 3);
        Set<GrantAndPermission> grantSet = new HashSet<GrantAndPermission>();
        for (int i = 1; i <= 3; ++i) {
            grantSet.add(new GrantAndPermission(new CanonicalGrantee(Integer.toString(i)), Permission.PERMISSION_READ));
        }
    }
}
