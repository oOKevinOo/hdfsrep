package org.pentaho.di.repository.hdfsrep;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleSecurityException;
import org.pentaho.di.repository.IUser;
import org.pentaho.di.repository.RepositoryOperation;
import org.pentaho.di.repository.RepositorySecurityProvider;

import java.util.List;

/**
 * Created by zhiwen wang on 2017/9/29.
 */
public class KettleHDFSRepositorySecurityProvider implements RepositorySecurityProvider {
    @Override
    public IUser getUserInfo() {
        return null;
    }

    @Override
    public void validateAction(RepositoryOperation... operations) throws KettleException, KettleSecurityException {

    }

    @Override
    public boolean isReadOnly() {
        return false;
    }

    @Override
    public boolean isLockingPossible() {
        return false;
    }

    @Override
    public boolean allowsVersionComments(String fullPath) {
        return false;
    }

    @Override
    public boolean isVersionCommentMandatory() {
        return false;
    }

    @Override
    public List<String> getAllUsers() throws KettleException {
        return null;
    }

    @Override
    public List<String> getAllRoles() throws KettleException {
        return null;
    }

    @Override
    public String[] getUserLogins() throws KettleException {
        return new String[0];
    }

    @Override
    public boolean isVersioningEnabled(String fullPath) {
        return false;
    }
}
