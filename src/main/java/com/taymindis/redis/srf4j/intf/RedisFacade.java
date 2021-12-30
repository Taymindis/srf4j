package com.taymindis.redis.srf4j.intf;


public interface RedisFacade extends AutoCloseable{
    Session createSession();

    /** Read only Slave session,
     * This session is facade a slave/replica node for read
     * BENEFIT: All replica node still can working for those heavy read process
     * only for sentinel configuration**/
    Session createSlaveSession();

    <K,V> PubSubSession<K,V> createPubSubSession(PubSubEvent<K, V> pubSubEvent);

    <V> ListSession<V> useListSession(String key);

/* TODO
 *
    <V> SetSession<V> createSetSession();
    <V> SortSetSession<V> createSortedSetSession();
    <V> HashSession<V> createHashSession();
 **/
    Object getCore();

}
