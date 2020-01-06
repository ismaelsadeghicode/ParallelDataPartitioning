package com.parallel.dao.basical;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * @author Ismael Sadeghi, 2020-01-06 19:41
 */
public interface Dao<E, ID> {
    public <S extends E> S save(S entity) throws Exception;

    public <S extends E> List<S> save(Iterable<S> entities) throws Exception;

    public <S extends E> S update(S entity);

    public <S extends E> List<S> update(Iterable<S> entities) throws Exception;

    public Optional<E> select(ID id) throws Exception;

    public List<E> select() throws Exception;

    public Iterable<E> select(List<ID> ids) throws Exception;

    public Iterable<E> select(int first, int max) throws Exception;

    public <S extends E> S delete(S entity) throws Exception;

    public long delete() throws Exception;

    public long delete(Iterable<E> entities) throws Exception;

    public boolean deleteByID(ID id) throws Exception;

    public long getCount() throws Exception;

    public Iterable<Object> selectMultiType(String queryName, Map<String, Object> params) throws Exception;

    public Iterable<Object> selectMultiType(String queryName, Map<String, Object> params, int first, int max)
            throws Exception;

    public Iterable<E> select(String queryName, Map<String, Object> params, int first, int max) throws Exception;

    public Iterable<E> executeNamedQuery(String queryName, Map<String, Object> params) throws Exception;

    public Iterable<Object> executeQuery(String strQuery) throws Exception;


}
