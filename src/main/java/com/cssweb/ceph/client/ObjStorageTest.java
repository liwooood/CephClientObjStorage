package com.cssweb.ceph.client;

import java.io.File;
import java.util.List;
import java.util.Map;
import com.rackspacecloud.client.cloudfiles.FilesClient;
import com.rackspacecloud.client.cloudfiles.FilesConstants;
import com.rackspacecloud.client.cloudfiles.FilesContainer;
import com.rackspacecloud.client.cloudfiles.FilesContainerExistsException;
import com.rackspacecloud.client.cloudfiles.FilesObject;
import com.rackspacecloud.client.cloudfiles.FilesObjectMetaData;

/**
 * Created by chenhf on 14-3-12.
 * http://ceph.com/docs/master/radosgw/swift/java/
 */
public class ObjStorageTest {

    String username = "USERNAME";
    String password = "PASSWORD";
    String authUrl  = "https://objects.dreamhost.com/auth";

    FilesClient client = new FilesClient(username, password, authUrl);
    if (!client.login()) {
        throw new RuntimeException("Failed to log in");
    }

    client.createContainer("my-new-container");


    File file = new File("foo.txt");
    String mimeType = FilesConstants.getMimetype("txt");
    client.storeObject("my-new-container", file, mimeType);


    FilesObjectMetaData metaData = client.getObjectMetaData("my-new-container", "foo.txt");
    metaData.addMetaData("key", "value");

    Map<String, String> metamap = metaData.getMetaData();
    client.updateObjectMetadata("my-new-container", "foo.txt", metamap);


    List<FilesContainer> containers = client.listContainers();
    for (FilesContainer container : containers) {
        System.out.println("  " + container.getName());
    }


    List<FilesObject> objects = client.listObjects("my-new-container");
    for (FilesObject object : objects) {
        System.out.println("  " + object.getName());
    }

    FilesObject obj;
    File outfile = new File("outfile.txt");

    List<FilesObject> objects = client.listObjects("my-new-container");
    for (FilesObject object : objects) {
        String name = object.getName();
        if (name.equals("foo.txt")) {
            obj = object;
            obj.writeObjectToFile(outfile);
        }
    }

    client.deleteObject("my-new-container", "goodbye.txt");

    client.deleteContainer("my-new-container");
}
