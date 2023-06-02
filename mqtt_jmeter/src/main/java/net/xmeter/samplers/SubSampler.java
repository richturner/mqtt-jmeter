package net.xmeter.samplers;

import net.xmeter.SubBean;
import net.xmeter.samplers.mqtt.MQTTConnection;
import net.xmeter.samplers.mqtt.MQTTQoS;
import org.apache.jmeter.samplers.Entry;
import org.apache.jmeter.samplers.SampleResult;
import org.apache.jmeter.threads.JMeterContextService;
import org.apache.jmeter.threads.JMeterVariables;

import java.text.MessageFormat;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import net.xmeter.SubBean;
import net.xmeter.samplers.mqtt.MQTTConnection;
import net.xmeter.samplers.mqtt.MQTTQoS;
import org.apache.jmeter.samplers.Entry;
import org.apache.jmeter.samplers.SampleResult;
import org.apache.jmeter.threads.JMeterContextService;
import org.apache.jmeter.threads.JMeterVariables;

@SuppressWarnings("deprecation")
public class SubSampler extends AbstractMQTTSampler {
	private static final long serialVersionUID = 2979978053740194951L;
	private static final Logger logger = Logger.getLogger(SubSampler.class.getCanonicalName());
	
	private transient MQTTConnection connection = null;
	private transient String clientId;
	private boolean subFailed = false;

	private int sampleElapsedTime = 1000;
	private int sampleCount = 1;
	private int sampleCountTimeout = 5000;

	private final transient ConcurrentLinkedQueue<SubBean> batches = new ConcurrentLinkedQueue<>();
	private boolean printFlag = false;
	private boolean asyncActive = false;

	private final transient Object dataLock = new Object();
	private boolean lockReleased = false;

	public String getQOS() {
		return getPropertyAsString(QOS_LEVEL, String.valueOf(QOS_0));
	}

	public void setQOS(String qos) {
		setProperty(QOS_LEVEL, qos);
	}

	public String getTopic() { return getPropertyAsString(TOPIC_NAME, DEFAULT_TOPIC_NAME); }

	public void setTopic(String topicsName) {
		setProperty(TOPIC_NAME, topicsName);
	}
	
	public String getSampleCondition() {
		return getPropertyAsString(SAMPLE_CONDITION, SAMPLE_ON_CONDITION_OPTION1);
	}
	
	public void setSampleCondition(String option) {
		setProperty(SAMPLE_CONDITION, option);
	}
	
	public String getSampleCount() {
		return getPropertyAsString(SAMPLE_CONDITION_VALUE, DEFAULT_SAMPLE_VALUE_COUNT);
	}
	
	public void setSampleCount(String count) {
		setProperty(SAMPLE_CONDITION_VALUE, count);
	}

	public String getSampleCountTimeout() {
		return getPropertyAsString(SAMPLE_CONDITION_VALUE_OPT, DEFAULT_SAMPLE_VALUE_COUNT_TIMEOUT);
	}

	public void setSampleCountTimeout(String timeout) {
		setProperty(SAMPLE_CONDITION_VALUE_OPT, timeout);
	}

	public String getSampleElapsedTime() {
		return getPropertyAsString(SAMPLE_CONDITION_VALUE, DEFAULT_SAMPLE_VALUE_ELAPSED_TIME_MILLI_SEC);
	}
	
	public void setSampleElapsedTime(String elapsedTime) {
		setProperty(SAMPLE_CONDITION_VALUE, elapsedTime);
	}

	public boolean isAddTimestamp() {
		return getPropertyAsBoolean(ADD_TIMESTAMP);
	}

	public void setAddTimestamp(boolean addTimestamp) {
		setProperty(ADD_TIMESTAMP, addTimestamp);
	}

	public boolean isDebugResponse() {
		return getPropertyAsBoolean(DEBUG_RESPONSE, false);
	}

	public void setDebugResponse(boolean debugResponse) {
		setProperty(DEBUG_RESPONSE, debugResponse);
	}

	public boolean isAsync() {
		return getPropertyAsBoolean(SUB_ASYNC, false);
	}

	public void setAsync(boolean debugResponse) {
		setProperty(SUB_ASYNC, debugResponse);
	}

	@Override
	public SampleResult sample(Entry arg0) {
		SampleResult result = new SampleResult();
		result.setSampleLabel(getName());
		JMeterVariables vars = JMeterContextService.getContext().getVariables();
		connection = (MQTTConnection) vars.getObject(getConnName());
		clientId = (String) vars.getObject(getConnName()+"_clientId");
		if (connection == null) {
			return produceResult(result, 503, "Subscribe failed because connection is not established");
		}

		// initial values
		boolean sampleByTime = SAMPLE_ON_CONDITION_OPTION1.equals(getSampleCondition());
		try {
			if (sampleByTime) {
				sampleElapsedTime = Integer.parseUnsignedInt(getSampleElapsedTime());
			} else {
				sampleCount = Integer.parseUnsignedInt(getSampleCount());
				sampleCountTimeout = Integer.parseUnsignedInt(getSampleCountTimeout());
			}
		} catch (NumberFormatException e) {
			return produceResult(result, 400, "Unrecognized value for sample elapsed time or message count");
		}
		
		if (sampleByTime && sampleElapsedTime <=0) {
			return produceResult(result, 400, "Sample on elapsed time: must be greater than 0 ms");
		} else if (sampleCount < 1) {
			return produceResult(result, 400, "Sample on message count: must be greater than 0");
		}
		
		String topicsName = getTopic();
		setListener(sampleByTime, sampleCount);
		Set<String> topics = topicsSubscribed.get(clientId);
		if (topics == null) {
			logger.severe("subscribed topics haven't been initiated. [clientId: " + (clientId == null ? "null" : clientId) + "]");
			topics = new HashSet<>();
			topics.add(topicsName);
			topicsSubscribed.put(clientId, topics);
			listenToTopics(topicsName);
		} else {
			if (!topics.contains(topicsName)) {
				topics.add(topicsName);
				topicsSubscribed.put(clientId, topics);
				logger.fine("Listen to topic: " + topicsName);
				result.sampleStart();
				listenToTopics(topicsName);
			}
		}
		
		if (subFailed) {
			// avoid massive repeated "early stage" failures in a short period of time
			// which probably overloads JMeter CPU and distorts test metrics such as TPS, avg response time
			try {
				TimeUnit.MILLISECONDS.sleep(SUB_FAIL_PENALTY);
			} catch (InterruptedException e) {
				logger.log(Level.INFO, "Received exception when waiting for notification signal", e);
			}

			return produceResult(result, 405, "Failed to subscribe to topic(s):" + topicsName);
		}

		return doSample(result);
	}

	private SampleResult doSample(SampleResult result) {
		JMeterVariables vars = JMeterContextService.getContext().getVariables();
		boolean sampleByTime = SAMPLE_ON_CONDITION_OPTION1.equals(getSampleCondition());

		if (isAsync() && !asyncActive) {
			asyncActive = true;
			vars.putObject("sub", this); // save this sampler as thread local variable for response sampler
			return produceResult(result, 200, "Subscription created");
		} else if (sampleByTime) {
			try {
				TimeUnit.MILLISECONDS.sleep(sampleElapsedTime);
			} catch (InterruptedException e) {
				logger.log(Level.INFO, "Received exception when waiting for notification signal", e);
				return produceResult(result, 408, "Interrupted");
			}
			synchronized (dataLock) {
				result.sampleStart();
				return produceResult(result, 200, null);
			}
		} else {
			synchronized (dataLock) {
				result.sampleStart();
				int receivedCount = (batches.isEmpty() ? 0 : batches.element().getReceivedCount());
				if (receivedCount < sampleCount) {
					try {
						if (sampleCountTimeout > 0) {
							// handle spurious wakeups (https://docs.oracle.com/en/java/javase/18/docs/api/java.base/java/lang/Object.html#wait(long,int))
							long start = System.currentTimeMillis();
							long sleepMillis = sampleCountTimeout;

							while (!lockReleased && sleepMillis > 0) {
								dataLock.wait(sleepMillis);
								sleepMillis = sampleCountTimeout - (System.currentTimeMillis() - start);
							}
						} else {
							// handle spurious wakeups (https://docs.oracle.com/en/java/javase/18/docs/api/java.base/java/lang/Object.html#wait(long,int))
							while (!lockReleased) {
								dataLock.wait();
							}
						}
					} catch (InterruptedException e) {
						logger.log(Level.INFO, "Received exception when waiting for notification signal", e);
						return produceResult(result, 408, "Interrupted");
					}
				}
				receivedCount = (batches.isEmpty() ? 0 : batches.element().getReceivedCount());
				if (receivedCount < sampleCount) {
					result.setSampleCount(receivedCount);
					return produceResult(result, 408, null);
				}
				return produceResult(result, 200, null);
			}
		}
	}

	protected SampleResult produceAsyncResult(SampleResult result, boolean clearResponses) {
		synchronized (dataLock) {
			SampleResult result1 = doSample(result);
			if (clearResponses) {
				batches.clear();
			}
			lockReleased = false;
			return result1;
		}
	}

	private SampleResult produceResult(SampleResult result, int responseCode, String message) {
		boolean sampleByTime = SAMPLE_ON_CONDITION_OPTION1.equals(getSampleCondition());
		SubBean bean = batches.poll();

		if (bean == null) {
			bean = new SubBean();
		}

		int receivedCount = bean.getReceivedCount();
		List<String> contents = bean.getContents();

		if (message == null) {
			if (sampleByTime) {
				message = MessageFormat.format("Received {0} message(s) in {1}ms", receivedCount, sampleElapsedTime);
			} else {
				message = MessageFormat.format("Received {0} message(s) of expected {1} message(s)", receivedCount, sampleCount);
			}
		}

		StringBuilder content = new StringBuilder();
		if (isDebugResponse()) {
			for (String s : contents) {
				content.append(s).append("\n");
			}
		}

		result.setResponseCode(Integer.toString(responseCode));
		// Any response code in 200 range is successful (like HTTP)
		result.setSuccessful(responseCode / 100 == 2);
		result.setResponseMessage(message);
		result.setBodySize(bean.getReceivedMessageSize());
		result.setBytes(bean.getReceivedMessageSize());
		result.setResponseData(contents.toString().getBytes());
		result.setSampleCount(receivedCount);
		result.sampleEnd();

		return result;
	}
	
	private void listenToTopics(final String topicsName) {
		int qos;
		try {
			qos = Integer.parseInt(getQOS());
		} catch(Exception e) {
			logger.log(Level.SEVERE, e, () -> MessageFormat.format("Specified invalid QoS value {0}, set to default QoS value {1}!", e.getMessage(), QOS_0));
			qos = QOS_0;
		}
		
		final String[] topicNames = topicsName.split(",");
		if(qos < 0 || qos > 2) {
			logger.severe("Specified invalid QoS value, set to default QoS value " + qos);
			qos = QOS_0;
		}

		connection.subscribe(topicNames,
				MQTTQoS.fromValue(qos),
				() -> {
					logger.fine(() -> "successful subscribed topics: " + String.join(", ", topicNames));
					synchronized (dataLock) {
						dataLock.notify();
					}
				},
				error -> {
					logger.log(Level.INFO, "subscribe failed topics: " + topicsName, error);
					subFailed = true;

					synchronized (dataLock) {
						dataLock.notify();
					}
				});

		// Block until response
		synchronized (dataLock) {
			try {
				dataLock.wait();
			} catch (InterruptedException e) {
				subFailed = true;
			}
		}
	}
	
	private void setListener(final boolean sampleByTime, final int sampleCount) {
		connection.setSubListener(((topic, message, ack) -> {
			ack.run();

			if(sampleByTime) {
				synchronized (dataLock) {
					handleSubBean(sampleByTime, message, sampleCount);
				}
			} else {
				synchronized (dataLock) {
					SubBean bean = handleSubBean(sampleByTime, message, sampleCount);
					if(bean.getReceivedCount() == sampleCount) {
						lockReleased = true;
						dataLock.notify();
					}
				}
			}
		}));
	}
	
	private SubBean handleSubBean(boolean sampleByTime, String msg, int sampleCount) {
		SubBean bean;
		if(batches.isEmpty()) {
			bean = new SubBean();
			batches.add(bean);
		} else {
			SubBean[] beans = new SubBean[batches.size()];
			batches.toArray(beans);
			bean = beans[beans.length - 1];
		}
		
		if((!sampleByTime) && (bean.getReceivedCount() == sampleCount)) { //Create a new batch when latest bean is full.
			logger.info("The tail bean is full, will create a new bean for it.");
			bean = new SubBean();
			batches.add(bean);
		}
		if (isAddTimestamp()) {
			long now = System.currentTimeMillis();
			int index = msg.indexOf(TIME_STAMP_SEP_FLAG);
			if (index == -1 && (!printFlag)) {
				logger.info(() -> "Payload does not include timestamp: " + msg);
				printFlag = true;
			} else if (index != -1) {
				long start = Long.parseLong(msg.substring(0, index));
				long elapsed = now - start;
				
				double avgElapsedTime = bean.getAvgElapsedTime();
				int receivedCount = bean.getReceivedCount();
				avgElapsedTime = (avgElapsedTime * receivedCount + elapsed) / (receivedCount + 1);
				bean.setAvgElapsedTime(avgElapsedTime);
			}
		}
		if (isDebugResponse()) {
			bean.getContents().add(msg);
		}
		bean.setReceivedMessageSize(bean.getReceivedMessageSize() + msg.length());
		bean.setReceivedCount(bean.getReceivedCount() + 1);
		return bean;
	}
}
