package com.netflix.suro.sink.remotefile;

import com.google.common.base.Strings;
import org.jets3t.service.ServiceException;
import org.jets3t.service.acl.AccessControlList;
import org.jets3t.service.acl.CanonicalGrantee;
import org.jets3t.service.acl.Permission;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.model.S3Object;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * When uploading files to S3 with opening up to the certain AWS id, this can be
 * used
 *
 * @author jbae
 */
public class GrantAcl {
    static Logger log = LoggerFactory.getLogger(GrantAcl.class);

    private final RestS3Service s3Service;
    private final String s3Acl;
    private final int s3AclRetries;

    public GrantAcl(RestS3Service s3Service, String s3Acl, int s3AclRetries) {
        this.s3Service = s3Service;
        this.s3Acl = s3Acl;
        this.s3AclRetries = s3AclRetries;
    }

    public boolean grantAcl(S3Object object) throws ServiceException, InterruptedException {
        if (Strings.isNullOrEmpty(s3Acl) == false) {
            for (int i = 0; i < s3AclRetries; ++i) {
                try {
                    AccessControlList acl = s3Service.getObjectAcl(object.getBucketName(), object.getKey());
                    for (String id : s3Acl.split(",")) {
                        acl.grantPermission(new CanonicalGrantee(id), Permission.PERMISSION_READ);
                    }
                    s3Service.putObjectAcl(object.getBucketName(), object.getKey(), acl);
                    return true;
                } catch (Exception e) {
                    log.error("Exception while granting ACL: " + e.getMessage());
                    Thread.sleep(1000 * (i + 1));
                }
            }
            return false;
        } else {
            return true;
        }
    }
}
