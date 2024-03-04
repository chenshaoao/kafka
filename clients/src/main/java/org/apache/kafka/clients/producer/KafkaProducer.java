
package org.apache.kafka.clients.producer;

import org.apache.kafka.clients.*;
import org.apache.kafka.clients.producer.internals.*;
import org.apache.kafka.common.*;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.network.*;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.common.utils.KafkaThread;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.nio.ch.SocketChannelImpl;

import java.net.InetSocketAddress;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A Kafka client that publishes records to the Kafka cluster.
 * <P>
 * The producer is <i>thread safe</i> and sharing a single producer instance across threads will generally be faster than
 * having multiple instances.
 * <p>
 * Here is a simple example of using the producer to send records with strings containing sequential numbers as the key/value
 * pairs.
 * <pre>
 * {@code
 * Properties props = new Properties();
 * props.put("bootstrap.servers", "localhost:9092");
 * props.put("acks", "all");
 * props.put("retries", 0);
 * props.put("batch.size", 16384);
 * props.put("linger.ms", 1);
 * props.put("buffer.memory", 33554432);
 * props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
 * props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
 *
 * Producer<String, String> producer = new KafkaProducer<>(props);
 * for(int i = 0; i < 100; i++)
 *     producer.send(new ProducerRecord<String, String>("my-topic", Integer.toString(i), Integer.toString(i)));
 *
 * producer.close();
 * }</pre>
 * <p>
 * The producer consists of a pool of buffer space that holds records that haven't yet been transmitted to the server
 * as well as a background I/O thread that is responsible for turning these records into requests and transmitting them
 * to the cluster. Failure to close the producer after use will leak these resources.
 * <p>
 * The {@link #send(ProducerRecord) send()} method is asynchronous. When called it adds the record to a buffer of pending record sends
 * and immediately returns. This allows the producer to batch together individual records for efficiency.
 * <p>
 * The <code>acks</code> config controls the criteria under which requests are considered complete. The "all" setting
 * we have specified will result in blocking on the full commit of the record, the slowest but most durable setting.
 * <p>
 * If the request fails, the producer can automatically retry, though since we have specified <code>retries</code>
 * as 0 it won't. Enabling retries also opens up the possibility of duplicates (see the documentation on
 * <a href="http://kafka.apache.org/documentation.html#semantics">message delivery semantics</a> for details).
 * <p>
 * The producer maintains buffers of unsent records for each partition. These buffers are of a size specified by
 * the <code>batch.size</code> config. Making this larger can result in more batching, but requires more memory (since we will
 * generally have one of these buffers for each active partition).
 * <p>
 * By default a buffer is available to send immediately even if there is additional unused space in the buffer. However if you
 * want to reduce the number of requests you can set <code>linger.ms</code> to something greater than 0. This will
 * instruct the producer to wait up to that number of milliseconds before sending a request in hope that more records will
 * arrive to fill up the same batch. This is analogous to Nagle's algorithm in TCP. For example, in the code snippet above,
 * likely all 100 records would be sent in a single request since we set our linger time to 1 millisecond. However this setting
 * would add 1 millisecond of latency to our request waiting for more records to arrive if we didn't fill up the buffer. Note that
 * records that arrive close together in time will generally batch together even with <code>linger.ms=0</code> so under heavy load
 * batching will occur regardless of the linger configuration; however setting this to something larger than 0 can lead to fewer, more
 * efficient requests when not under maximal load at the cost of a small amount of latency.
 * <p>
 * The <code>buffer.memory</code> controls the total amount of memory available to the producer for buffering. If records
 * are sent faster than they can be transmitted to the server then this buffer space will be exhausted. When the buffer space is
 * exhausted additional send calls will block. The threshold for time to block is determined by <code>max.block.ms</code> after which it throws
 * a TimeoutException.
 * <p>
 * The <code>key.serializer</code> and <code>value.serializer</code> instruct how to turn the key and value objects the user provides with
 * their <code>ProducerRecord</code> into bytes. You can use the included {@link org.apache.kafka.common.serialization.ByteArraySerializer} or
 * {@link org.apache.kafka.common.serialization.StringSerializer} for simple string or byte types.
 */

/**
 *
 * 亮眼标题：
 * 1. 阿里P8说没有看过这段Kafka源码（内容核心是讲清楚源码讲解思路）
 * 2. Kafka消息发送源码分析，不用（没有）万字长文（内容核心是讲清楚框架大图）（结合AI）
 *  评论找托，初始化内容，再写一篇9999子的长文。
 * 2. Kafka黑话（内容核心是讲清楚Kafka核心概念和核心方法，对应的英文名词）
 *
 * 群：开源源码群，付费教程群，和群主说自己要分析文章，同意后群里发送。
 * 准备好品牌账号。
 * 声音处理。
 * 内容结合AI。
 * 官网onboard，让顾客着陆。
 * 关注盗版（了解一下），文章里只给客服，不给官网，否则会认为打广告。
 * 免费也签约，使用微信专栏1元付费。
 * 成员加上武宁，比较靠谱。
 *
 * 快捷键：
 * 代码提交： g + gp
 * 锚点定位：ctrl + 数字
 * cmd 1 2(收藏的代码） 3 7
 * ^H 看方法实现类
 *
 * import 里引入了全路径，注释里就不用写全路径了。
 * 全局类图，核心类注释一定要好好看看。
 *
 * 差异化：
 * 1. 给出逻辑框架（不要直接陷入细节）（前世今生，为什么这么写）
 * 2. 总结代码模板，其他教程讲完了就让学生自己理解了，好的总结很重要（这个是差异化，对0基础的人非常友好）
 * 3. 精准定位，让你不再成为无头苍蝇（无限的时间浪费，不知道处理逻辑在哪，呼应逻辑在哪）（AI时代，拼逻辑本质，运用能力，拯救你的时间）
 * 4. 核心技术是不会过时的（世界级代码，小马哥的运维项目2016年就写好了）
 * 5. 给出代码注释版的前提，还提供IDEA收藏脚本，0门槛启动代码阅读（这个是Pro 用户才有的）（很多用户在环境上卡死了）
 * 6. 视频和直播伴读（这个是Pro用户才有的，现场回答疑问，总会有疑问的）按次收取课时费用，专题解答。首次1元1小时。有用再按折扣收取。总监不同价格。手把手，没有这个模式的。
 * 7. client 的源码解析全免费，服务端的源码解析收费。学习从简单开始，技术领域专注源码和业务场景结合。
 *
 * 多线程代码模版：
 * @see KafkaProducer#waitOnMetadata
 * @see RecordAccumulator#append
 *
 *
 * 重试代码模板：
 * @see Metadata#awaitUpdate
 *
 * 异常处理：
 * 底层处理异常上抛，核心流程统一处理异常。
 * @see Metadata#awaitUpdate
 *
 * 业务线程：
 * @see KafkaProducer#KafkaProducer【1】
 * @see KafkaProducer#doSend【2】
 *      @see KafkaProducer#waitOnMetadata
 *      @see KafkaProducer#partition
 *      @see RecordAccumulator#append
 *          @see RecordAccumulator#tryAppend
 *              @see RecordBatch#tryAppend 单条记录添加到批次。TODO full 场景梳理
 *          @see BufferPool#allocate
 *          @see RecordBatch#RecordBatch
 * 命名专题：
 * @see Sender#run(long) metadata.fetch() 命名很不好。
 *
 * 核心概念专题：核心方法专题
 *
 * 处理框架：
 * 1. 封装请求
 * 2. 发送请求
 * 3. 处理响应
 * 写作： 输入，思考，输出
 * 业务/研发： 需求，执行，验收
 * 闭环： 沟通，执行，反馈
 * 复盘： 事前，事中，事后（审视控制点）
 * 计算机： 输入设备，操作系统，输出设备
 *
 * 处理过程：
 * 存储关联，存储映射，绑定关系，绑定数据
 *
 * @see NetworkClient#poll【4】
 *
 * 代码分层：
 * - 业务层：KafkaProducer，Sender
 * - 工具层：NetworkClient
 * - 网络层：Selector
 *
 * IO线程：
 * @see Sender#run(long)【3】
 *      @see RecordAccumulator#ready
 *      @see RecordAccumulator#drain
 *      @see Sender#createProduceRequests
 *      @see Sender#produceRequest
 *      @see NetworkClient#send
 *          @see NetworkClient#doSend
 *              @see Selector#send
 *                  @see KafkaChannel#setSend
 *      @see NetworkClient#poll【4】 （细化）请求和响应。元数据和消息数据。
 *
 * 元数据更新全流程：09-03 17 分钟导航的那个是怎么弄的，最后几分钟很重要，19 分钟核心。
 * 不用讲网络，也能把更新流程讲完。说明分层思维的重要性。
 *
 *
 * Metadata 等待请求：（行为暂存）（这个也是多线程处理模板）
 * @see KafkaProducer#doSend
 *      @see KafkaProducer#waitOnMetadata
 *          @see Metadata#awaitUpdate(int, long)
 *
 * Metadata 封装请求：（消息处理过程中，判断是否需要发送元数据更新请求）
 * @see Sender#run(long)
 *      @see NetworkClient#poll
 *          @see NetworkClient.DefaultMetadataUpdater#maybeUpdate(long)
 *              @see NetworkClient.DefaultMetadataUpdater#maybeUpdate(long, Node)
 *                  @see NetworkClient.DefaultMetadataUpdater#request （请求体是相同的）
 *              @see NetworkClient#doSend(ClientRequest, long) （暂存，绑定channel）
 *                  @see InFlightRequests#add(ClientRequest)
 *                  @see Selector#send(Send)
 *
 * Metadata 处理响应：(唤醒等待）
 * @see Sender#run(long)
 *      @see NetworkClient#poll
 *           @see Selector#poll(long) 发送请求
 *           @see NetworkClient#handleCompletedReceives
 *              @see NetworkClient#parseResponse 解析响应体
 *                  @see NetworkClient#correlate 关联请求和响应（TODO 如何1对1对上的）
 *              @see NetworkClient.DefaultMetadataUpdater#maybeHandleCompletedReceive 响应处理
 *                  @see NetworkClient.DefaultMetadataUpdater#handleResponse
 *                      @see Metadata#update(Cluster, long) 更新集群信息，唤醒等待线程
 *
 * 差异是消息放 response 里面，循环回调
 * 内存分配的讲解
 * selector io 层，请求和响应没有写，单独写
 * NetworkClient#poll
 * Selector#poll 分开讲
 * 请求序列化，响应序列化
 * 重试、超时
 * 请求和响应对应
 *
 * 暂存专题，行为暂存，数据暂存。（线程挂起、内存append）（使用到的数据结构：Metadata，RecordAccumulator）
 * 回调专题，行为回调，数据回调。（FutureRecordMetadata）
 *
 * 消息发送-等待请求：（数据暂存）
 * @see KafkaProducer#doSend
 *      @see RecordAccumulator#append （⭐️这块开专题讲）
 *          @see RecordAccumulator#tryAppend
 *          @see BufferPool#allocate(int, long) （⭐️这块开专题讲）
 *
 * 消息发送-封装请求
 * @see Sender#run(long)
 *      @see RecordAccumulator#ready
 *      @see RecordAccumulator#drain
 *      @see Sender#createProduceRequests
 *          @see Sender#produceRequest （这里设置了 callback 的逻辑）
 *      @see NetworkClient#send
 *          @see NetworkClient#doSend(ClientRequest, long) （暂存，绑定channel）
 *              @see InFlightRequests#add(ClientRequest)
 *              @see Selector#send(Send)
 *
 * 消息发送-处理响应
 * @see Sender#run(long)
 *      @see NetworkClient#poll
 *           @see Selector#poll(long) 发送请求
 *           @see NetworkClient#handleCompletedReceives
 *              @see NetworkClient#parseResponse 解析响应体
 *                  @see NetworkClient#correlate 关联请求和响应（TODO 如何1对1对上的）
 *              @see responses.add(ClientResponse) 添加响应
 *           @see RequestCompletionHandler#onComplete(ClientResponse) （callback 回调）
 *              @see Sender#handleProduceResponse
 *                  @see Sender#completeBatch （⭐️这块开专题讲）（移除已经接收响应的请求）（4个数据结构）
 *
 *
 * private final Map<String, KafkaChannel> channels;
 * private final List<Send> completedSends;
 * private final List<NetworkReceive> completedReceives;
 *
 * 专题：Selector#poll(long)，发送请求
 * @see Selector#poll(long)
 *      @see Selector#select(long)
 *      @see Selector#pollSelectionKeys
 *      // 处理连接
 *      // 处理读
 *      // 处理写
 *
 *
 *
 *
 *
 * 连接状态维护：
 * 建立连接
 * @see Sender#run(long)
 *      @see NetworkClient#ready(Node, long)
 *           @see ClusterConnectionStates#canConnect(java.lang.String, long)
 *           @see NetworkClient#initiateConnect(Node, long)
 *               @see Selector#connect(java.lang.String, java.net.InetSocketAddress, int, int)
 *                   @see SocketChannelImpl#connect(java.net.SocketAddress)
 *                   @see SelectableChannel#register(java.nio.channels.Selector, int)
 *                   @see PlaintextChannelBuilder#buildChannel(java.lang.String, java.nio.channels.SelectionKey, int)
 *                   @see SelectionKey#attach(java.lang.Object)
 *                   @see immediatelyConnectedKeys.add(key) 如果连接就将 key 加入，poll 的时候立即处理
 *
 * 连接超时
 * 断开连接
 *
 */
public class KafkaProducer<K, V> implements Producer<K, V> {

    private static final Logger log = LoggerFactory.getLogger(KafkaProducer.class);
    private static final AtomicInteger PRODUCER_CLIENT_ID_SEQUENCE = new AtomicInteger(1);
    private static final String JMX_PREFIX = "kafka.producer";

    private String clientId;
    private final Partitioner partitioner;
    private final int maxRequestSize;
    private final long totalMemorySize;
    private final Metadata metadata;
    private final RecordAccumulator accumulator;
    private final Sender sender;
    private final Metrics metrics = null; // 初始化删除后，要设置为null
    private final Thread ioThread;
    private final CompressionType compressionType;
    private final Sensor errors = null; // 初始化删除后，要设置为null
    private final Time time;
    private final Serializer<K> keySerializer;
    private final Serializer<V> valueSerializer;
    private final ProducerConfig producerConfig;
    private final long maxBlockTimeMs;
    private final int requestTimeoutMs;
    private final ProducerInterceptors<K, V> interceptors;

    public KafkaProducer(Map<String, Object> configs) {
        this(new ProducerConfig(configs), null, null);
    }

    public KafkaProducer(Map<String, Object> configs, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        this(new ProducerConfig(ProducerConfig.addSerializerToConfig(configs, keySerializer, valueSerializer)),
             keySerializer, valueSerializer);
    }

    public KafkaProducer(Properties properties) {
        this(new ProducerConfig(properties), null, null);
    }

    public KafkaProducer(Properties properties, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        this(new ProducerConfig(ProducerConfig.addSerializerToConfig(properties, keySerializer, valueSerializer)),
             keySerializer, valueSerializer);
    }

    @SuppressWarnings({"unchecked", "deprecation"})
    private KafkaProducer(ProducerConfig config, Serializer<K> keySerializer, Serializer<V> valueSerializer) {


        try {

            /**
             *
             * NetworkClient 类比 Web 服务的 HttpClient。
             *
             * HttpClient 也需要这些初始化配置
             * retryBackoffMs、maxBlockTimeMs、requestTimeoutMs、maxRequestSize
             *
             * HttpClient 指定的域名和IP，Kafka 需要自己负载均衡，指定分区对应的主机
             *
             * HttpClient 也会指定序列化和压缩格式
             *
             * 所有的发送逻辑都是 NetworkClient 处理的，请求封装，响应处理。
             * @see NetworkClient#poll(long, long)
             *
             */
            // 用户自定义配置：初始化
            Map<String, Object> userProvidedConfigs = config.originals();
            this.producerConfig = config;
            this.time = new SystemTime();

            clientId = config.getString(ProducerConfig.CLIENT_ID_CONFIG);
            if (clientId.length() <= 0)
                clientId = "producer-" + PRODUCER_CLIENT_ID_SEQUENCE.getAndIncrement();
            // 用户自定义配置：设置客户端id
            userProvidedConfigs.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);

            // 网络的初始化配置

            // 重试时间间隔
            long retryBackoffMs = config.getLong(ProducerConfig.RETRY_BACKOFF_MS_CONFIG);

            // if else 是为了兼容历史版本
            if (userProvidedConfigs.containsKey(ProducerConfig.BLOCK_ON_BUFFER_FULL_CONFIG)) {
                boolean blockOnBufferFull = config.getBoolean(ProducerConfig.BLOCK_ON_BUFFER_FULL_CONFIG);
                if (blockOnBufferFull) {
                    this.maxBlockTimeMs = Long.MAX_VALUE;
                } else if (userProvidedConfigs.containsKey(ProducerConfig.METADATA_FETCH_TIMEOUT_CONFIG)) {
                    this.maxBlockTimeMs = config.getLong(ProducerConfig.METADATA_FETCH_TIMEOUT_CONFIG);
                } else {
                    this.maxBlockTimeMs = config.getLong(ProducerConfig.MAX_BLOCK_MS_CONFIG);
                }
            } else if (userProvidedConfigs.containsKey(ProducerConfig.METADATA_FETCH_TIMEOUT_CONFIG)) {
                this.maxBlockTimeMs = config.getLong(ProducerConfig.METADATA_FETCH_TIMEOUT_CONFIG);
            } else {
                this.maxBlockTimeMs = config.getLong(ProducerConfig.MAX_BLOCK_MS_CONFIG);
            }

            if (userProvidedConfigs.containsKey(ProducerConfig.TIMEOUT_CONFIG)) {
                this.requestTimeoutMs = config.getInt(ProducerConfig.TIMEOUT_CONFIG);
            } else {
                this.requestTimeoutMs = config.getInt(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG);
            }
            this.maxRequestSize = config.getInt(ProducerConfig.MAX_REQUEST_SIZE_CONFIG);
            this.totalMemorySize = config.getLong(ProducerConfig.BUFFER_MEMORY_CONFIG);

            // 步骤一：设置分区器（负载均衡）
            this.partitioner = config.getConfiguredInstance(ProducerConfig.PARTITIONER_CLASS_CONFIG, Partitioner.class);
            // 步骤二：设置序列化器
            if (keySerializer == null) {
                this.keySerializer = config.getConfiguredInstance(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                        Serializer.class);
                this.keySerializer.configure(config.originals(), true);
            } else {
                config.ignore(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG);
                this.keySerializer = keySerializer;
            }
            if (valueSerializer == null) {
                this.valueSerializer = config.getConfiguredInstance(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                        Serializer.class);
                this.valueSerializer.configure(config.originals(), false);
            } else {
                config.ignore(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG);
                this.valueSerializer = valueSerializer;
            }
            // 步骤三：设置拦截器
            List<ProducerInterceptor<K, V>> interceptorList = (List) (new ProducerConfig(userProvidedConfigs)).getConfiguredInstances(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,
                    ProducerInterceptor.class);
            this.interceptors = interceptorList.isEmpty() ? null : new ProducerInterceptors<>(interceptorList);

            ClusterResourceListeners clusterResourceListeners = configureClusterResourceListeners(keySerializer, valueSerializer, interceptorList, null);

            // 步骤四：初始化集群元数据
            this.metadata = new Metadata(retryBackoffMs, config.getLong(ProducerConfig.METADATA_MAX_AGE_CONFIG), true, clusterResourceListeners);

            // 步骤五：设置压缩格式
            this.compressionType = CompressionType.forName(config.getString(ProducerConfig.COMPRESSION_TYPE_CONFIG));

            // 步骤六：设置消息累计器
            this.accumulator = new RecordAccumulator(config.getInt(ProducerConfig.BATCH_SIZE_CONFIG),
                    this.totalMemorySize,
                    this.compressionType,
                    config.getLong(ProducerConfig.LINGER_MS_CONFIG),
                    retryBackoffMs,
                    null, // 监控的不管
                    time);

            List<InetSocketAddress> addresses = ClientUtils.parseAndValidateAddresses(config.getList(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
            // metadata 初始化 Cluster 信息。 Cluster.bootstrap(addresses) 初始化主机Node编号和主机IP。
            this.metadata.update(Cluster.bootstrap(addresses), time.milliseconds());

            // 步骤七：初始化网络组件
            ChannelBuilder channelBuilder = ClientUtils.createChannelBuilder(config.values());
            NetworkClient client = new NetworkClient(
                    new Selector(config.getLong(ProducerConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG), this.metrics, time, "producer", channelBuilder),
                    this.metadata,
                    clientId,
                    config.getInt(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION),
                    config.getLong(ProducerConfig.RECONNECT_BACKOFF_MS_CONFIG),
                    config.getInt(ProducerConfig.SEND_BUFFER_CONFIG),
                    config.getInt(ProducerConfig.RECEIVE_BUFFER_CONFIG),
                    this.requestTimeoutMs, time);
            // 步骤八：设置发送处理逻辑
            this.sender = new Sender(client,
                    this.metadata,
                    this.accumulator,
                    config.getInt(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION) == 1,
                    config.getInt(ProducerConfig.MAX_REQUEST_SIZE_CONFIG),
                    (short) parseAcks(config.getString(ProducerConfig.ACKS_CONFIG)),
                    config.getInt(ProducerConfig.RETRIES_CONFIG),
                    this.metrics,
                    new SystemTime(),
                    clientId,
                    this.requestTimeoutMs);
            // 步骤九：初始化IO发送线程，并启动
            String ioThreadName = "kafka-producer-network-thread" + (clientId.length() > 0 ? " | " + clientId : "");
            this.ioThread = new KafkaThread(ioThreadName, this.sender, true);
            this.ioThread.start();
        } catch (Throwable t) {
            close(0, TimeUnit.MILLISECONDS, true);
            throw new KafkaException("Failed to construct kafka producer", t);
        }
    }

    private static int parseAcks(String acksString) {
        try {
            return acksString.trim().equalsIgnoreCase("all") ? -1 : Integer.parseInt(acksString.trim());
        } catch (NumberFormatException e) {
            throw new ConfigException("Invalid configuration value for 'acks': " + acksString);
        }
    }

    @Override
    public Future<RecordMetadata> send(ProducerRecord<K, V> record) {
        return send(record, null);
    }

    @Override
    public Future<RecordMetadata> send(ProducerRecord<K, V> record, Callback callback) {
        // intercept the record, which can be potentially modified; this method does not throw exceptions
        ProducerRecord<K, V> interceptedRecord = this.interceptors == null ? record : this.interceptors.onSend(record);
        return doSend(interceptedRecord, callback);
    }

    private Future<RecordMetadata> doSend(ProducerRecord<K, V> record, Callback callback) {
        TopicPartition tp = null;
        try {
            /**
             * 第一次发送消息时，如果没有元数据，要同步元数据，发送耗时会比较长，后面的就直接读缓存的。在算法中类似平均复杂度的场景，平均到每次发送就被抵消了。
             *
             * （TODO 第一次如果超时了，如何处理？抛异常？remainingWaitMs 在分配内存的时候也会使用的）
             *
             * 对于业务线程来说，maxBlockTimeMs 表示一次消息发送最大阻塞时间。拉取元数据阻塞和申请内存阻塞共享的这个阻塞时间。
             * 这里如果超时了，会调用回调方法，如果回调方法不处理，就会丢消息。（这里的回调执行线程是业务线程）
             * Kafka 消费的时候，只要异常处理合理，不会丢消息的。（关键看 offset 维护逻辑）
             *
             */
            // 步骤一：获取 topic 的集群元数据（第一次一定要有）
            ClusterAndWaitTime clusterAndWaitTime = waitOnMetadata(record.topic(), record.partition(), maxBlockTimeMs);
            long remainingWaitMs = Math.max(0, maxBlockTimeMs - clusterAndWaitTime.waitedOnMetadataMs);
            Cluster cluster = clusterAndWaitTime.cluster;

            // 步骤二：消息序列化
            byte[] serializedKey;
            try {
                serializedKey = keySerializer.serialize(record.topic(), record.key());
            } catch (ClassCastException cce) {
                throw new SerializationException("Can't convert key of class " + record.key().getClass().getName() + " to class " + producerConfig.getClass(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG).getName() + " specified in key.serializer");
            }
            byte[] serializedValue;
            try {
                serializedValue = valueSerializer.serialize(record.topic(), record.value());
            } catch (ClassCastException cce) {
                throw new SerializationException("Can't convert value of class " + record.value().getClass().getName() + " to class " + producerConfig.getClass(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG).getName() + " specified in value.serializer");
            }
            // 步骤三：计算分区（负载均衡）
            int partition = partition(record, serializedKey, serializedValue, cluster);
            // 步骤四：计算消息大小
            int serializedSize = Records.LOG_OVERHEAD + Record.recordSize(serializedKey, serializedValue);
            ensureValidRecordSize(serializedSize);
            tp = new TopicPartition(record.topic(), partition); // 消息要发到，哪个 topic ，哪个 partition
            long timestamp = record.timestamp() == null ? time.milliseconds() : record.timestamp();
            // 步骤五：设置回调函数
            Callback interceptCallback = this.interceptors == null ? callback : new InterceptorCallback<>(callback, this.interceptors, tp);
            // 步骤六：消息存储 ⭐
            RecordAccumulator.RecordAppendResult result = accumulator.append(tp, timestamp, serializedKey, serializedValue, interceptCallback, remainingWaitMs);
            // 批次满了才发，qps 1k，每次都发必挂，批次满了再发
            if (result.batchIsFull || result.newBatchCreated) {
                // 步骤七：如果批次满足发送条件，唤醒IO线程
                /**
                 * ⭐️⭐️⭐️ 多线程专题：IO线程阻塞唤醒
                 * @see org.apache.kafka.common.network.Selector#poll(long) 处理请求
                 * @see Selector#select(long) 阻塞
                 * @see org.apache.kafka.common.network.Selector#wakeup() 唤醒
                 */
                this.sender.wakeup();   // 业务线程 唤醒 IO线程。
            }
            // 步骤八：返回事件引用
            return result.future;
        } catch (ApiException e) {
            if (callback != null)
                callback.onCompletion(null, e);
            this.errors.record();
            if (this.interceptors != null)
                this.interceptors.onSendError(record, tp, e);
            return new FutureFailure(e);
        } catch (InterruptedException e) {
            this.errors.record();
            if (this.interceptors != null)
                this.interceptors.onSendError(record, tp, e);
            throw new InterruptException(e);
        } catch (BufferExhaustedException e) {
            this.errors.record();
            this.metrics.sensor("buffer-exhausted-records").record();
            if (this.interceptors != null)
                this.interceptors.onSendError(record, tp, e);
            throw e;
        } catch (KafkaException e) {
            this.errors.record();
            if (this.interceptors != null)
                this.interceptors.onSendError(record, tp, e);
            throw e;
        } catch (Exception e) {
            // we notify interceptor about all exceptions, since onSend is called before anything else in this method
            if (this.interceptors != null)
                this.interceptors.onSendError(record, tp, e);
            throw e;s
        }
    }


    private ClusterAndWaitTime waitOnMetadata(String topic, Integer partition, long maxWaitMs) throws InterruptedException {

        /**
         * ⭐️⭐️⭐ 多线程编程，代码模版。
         */
        /**
         * 多线程编程： try 简单逻辑，while 循环，重试逻辑，赋值逻辑相同。
         * 共同逻辑：获取集群、校验分区有效、返回结果。
         *
         *
         */
        /**
         *
         * 业务线程：设置更新状态，然后等待。
         * IO线程：实际执行网络请求。
         * 更新 Metadata.needUpdate = true; 通过 needUpdate 的使用，看触发逻辑。
         *
         * 更新 Metadata 流程：IO线程异步更新 Metadata，组装 MetadataRequest。
         * @see NetworkClient#poll(long, long)
         * @see NetworkClient.DefaultMetadataUpdater#maybeUpdate(long)
         *      @see Metadata#timeToNextUpdate
         * @see NetworkClient#handleCompletedReceives(java.util.List, long)
         * @see NetworkClient.DefaultMetadataUpdater#maybeHandleCompletedReceive
         * @see NetworkClient.DefaultMetadataUpdater#handleResponse
         *
         */

        // 把当前 topic 加入元数据 topic 列表
        metadata.add(topic);
        // 快速步骤一：获取集群缓存
        Cluster cluster = metadata.fetch();
        Integer partitionsCount = cluster.partitionCountForTopic(topic);
        // 快速步骤二：分区校验：分区存在，并且分区有效（中文非常简洁，博大精深）
        if (partitionsCount != null && (partition == null || partition < partitionsCount))
            // 快速步骤三：返回结果
            return new ClusterAndWaitTime(cluster, 0);

        long begin = time.milliseconds();
        long remainingWaitMs = maxWaitMs;
        long elapsed;
        do {
            // 更新 Metadata.needUpdate = true; IO线程异步更新。
            // KafkaProducer 初始化的时候，version+1 了，这里直接返回当前值。
            int version = metadata.requestUpdate();
            // 唤醒IO线程，处理请求
            sender.wakeup();
            try {
                // 标准步骤一：获取集群缓存。版本判断，条件不满足，循环等待。
                metadata.awaitUpdate(version, remainingWaitMs);
            } catch (TimeoutException ex) {
                throw new TimeoutException("Failed to update metadata after " + maxWaitMs + " ms.");
            }
            cluster = metadata.fetch();
            elapsed = time.milliseconds() - begin;
            if (elapsed >= maxWaitMs)
                throw new TimeoutException("Failed to update metadata after " + maxWaitMs + " ms.");
            if (cluster.unauthorizedTopics().contains(topic))
                throw new TopicAuthorizationException(topic);
            remainingWaitMs = maxWaitMs - elapsed; // 更新重试剩余时间
            partitionsCount = cluster.partitionCountForTopic(topic);
        } while (partitionsCount == null);

        // 正常执行完成。
        // 如果超时，前面 while 循环里已经抛异常了。

        // 标准步骤二：分区校验，分区无效
        if (partition != null && partition >= partitionsCount) {
            throw new KafkaException(String.format("Invalid partition given with record: %d is not in the range [0...%d).", partition, partitionsCount));
        }
        // 标准步骤三：返回结果
        return new ClusterAndWaitTime(cluster, elapsed);
    }


    private void ensureValidRecordSize(int size) {
        if (size > this.maxRequestSize)
            throw new RecordTooLargeException("The message is " + size + " bytes when serialized which is larger than the maximum request size you have configured with the " + ProducerConfig.MAX_REQUEST_SIZE_CONFIG + " configuration.");
        if (size > this.totalMemorySize)
            throw new RecordTooLargeException("The message is " + size + " bytes when serialized which is larger than the total memory buffer you have configured with the " + ProducerConfig.BUFFER_MEMORY_CONFIG + " configuration.");
    }

    @Override
    public void flush() {
        log.trace("Flushing accumulated records in producer.");
        this.accumulator.beginFlush();
        this.sender.wakeup();
        try {
            this.accumulator.awaitFlushCompletion();
        } catch (InterruptedException e) {
            throw new InterruptException("Flush interrupted.", e);
        }
    }

    @Override
    public List<PartitionInfo> partitionsFor(String topic) {
        try {
            return waitOnMetadata(topic, null, maxBlockTimeMs).cluster.partitionsForTopic(topic);
        } catch (InterruptedException e) {
            throw new InterruptException(e);
        }
    }

    @Override
    public Map<MetricName, ? extends Metric> metrics() {
        return Collections.unmodifiableMap(this.metrics.metrics());
    }

    @Override
    public void close() {
        close(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
    }

    @Override
    public void close(long timeout, TimeUnit timeUnit) {
        close(timeout, timeUnit, false);
    }

    private void close(long timeout, TimeUnit timeUnit, boolean swallowException) {
        if (timeout < 0)
            throw new IllegalArgumentException("The timeout cannot be negative.");

        log.info("Closing the Kafka producer with timeoutMillis = {} ms.", timeUnit.toMillis(timeout));
        // this will keep track of the first encountered exception
        AtomicReference<Throwable> firstException = new AtomicReference<Throwable>();
        boolean invokedFromCallback = Thread.currentThread() == this.ioThread;
        if (timeout > 0) {
            if (invokedFromCallback) {
                log.warn("Overriding close timeout {} ms to 0 ms in order to prevent useless blocking due to self-join. " +
                    "This means you have incorrectly invoked close with a non-zero timeout from the producer call-back.", timeout);
            } else {
                // Try to close gracefully.
                if (this.sender != null)
                    this.sender.initiateClose();
                if (this.ioThread != null) {
                    try {
                        this.ioThread.join(timeUnit.toMillis(timeout));
                    } catch (InterruptedException t) {
                        firstException.compareAndSet(null, t);
                        log.error("Interrupted while joining ioThread", t);
                    }
                }
            }
        }

        if (this.sender != null && this.ioThread != null && this.ioThread.isAlive()) {
            log.info("Proceeding to force close the producer since pending requests could not be completed " +
                "within timeout {} ms.", timeout);
            this.sender.forceClose();
            // Only join the sender thread when not calling from callback.
            if (!invokedFromCallback) {
                try {
                    this.ioThread.join();
                } catch (InterruptedException e) {
                    firstException.compareAndSet(null, e);
                }
            }
        }

        ClientUtils.closeQuietly(interceptors, "producer interceptors", firstException);
        ClientUtils.closeQuietly(metrics, "producer metrics", firstException);
        ClientUtils.closeQuietly(keySerializer, "producer keySerializer", firstException);
        ClientUtils.closeQuietly(valueSerializer, "producer valueSerializer", firstException);
        AppInfoParser.unregisterAppInfo(JMX_PREFIX, clientId);
        log.debug("The Kafka producer has closed.");
        if (firstException.get() != null && !swallowException)
            throw new KafkaException("Failed to close kafka producer", firstException.get());
    }

    private ClusterResourceListeners configureClusterResourceListeners(Serializer<K> keySerializer, Serializer<V> valueSerializer, List<?>... candidateLists) {
        ClusterResourceListeners clusterResourceListeners = new ClusterResourceListeners();
        for (List<?> candidateList: candidateLists)
            clusterResourceListeners.maybeAddAll(candidateList);

        clusterResourceListeners.maybeAdd(keySerializer);
        clusterResourceListeners.maybeAdd(valueSerializer);
        return clusterResourceListeners;
    }

    private int partition(ProducerRecord<K, V> record, byte[] serializedKey, byte[] serializedValue, Cluster cluster) {
        Integer partition = record.partition();
        return partition != null ?
                partition :
                partitioner.partition(
                        record.topic(), record.key(), serializedKey, record.value(), serializedValue, cluster);
    }

    private static class ClusterAndWaitTime {
        final Cluster cluster;
        final long waitedOnMetadataMs;
        ClusterAndWaitTime(Cluster cluster, long waitedOnMetadataMs) {
            this.cluster = cluster;
            this.waitedOnMetadataMs = waitedOnMetadataMs;
        }
    }

    private static class FutureFailure implements Future<RecordMetadata> {

        private final ExecutionException exception;

        public FutureFailure(Exception exception) {
            this.exception = new ExecutionException(exception);
        }

        @Override
        public boolean cancel(boolean interrupt) {
            return false;
        }

        @Override
        public RecordMetadata get() throws ExecutionException {
            throw this.exception;
        }

        @Override
        public RecordMetadata get(long timeout, TimeUnit unit) throws ExecutionException {
            throw this.exception;
        }

        @Override
        public boolean isCancelled() {
            return false;
        }

        @Override
        public boolean isDone() {
            return true;
        }

    }

    private static class InterceptorCallback<K, V> implements Callback {
        private final Callback userCallback;
        private final ProducerInterceptors<K, V> interceptors;
        private final TopicPartition tp;

        public InterceptorCallback(Callback userCallback, ProducerInterceptors<K, V> interceptors,
                                   TopicPartition tp) {
            this.userCallback = userCallback;
            this.interceptors = interceptors;
            this.tp = tp;
        }

        public void onCompletion(RecordMetadata metadata, Exception exception) {
            if (this.interceptors != null) {
                if (metadata == null) {
                    this.interceptors.onAcknowledgement(new RecordMetadata(tp, -1, -1, Record.NO_TIMESTAMP, -1, -1, -1),
                                                        exception);
                } else {
                    this.interceptors.onAcknowledgement(metadata, exception);
                }
            }
            if (this.userCallback != null)
                this.userCallback.onCompletion(metadata, exception);
        }
    }
}
