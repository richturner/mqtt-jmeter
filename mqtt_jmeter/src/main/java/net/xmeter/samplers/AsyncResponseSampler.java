package net.xmeter.samplers;

import org.apache.jmeter.samplers.Entry;
import org.apache.jmeter.samplers.SampleResult;
import org.apache.jmeter.threads.JMeterContextService;
import org.apache.jmeter.threads.JMeterVariables;

public class AsyncResponseSampler extends AbstractMQTTSampler {
	private static final long serialVersionUID = 4360869021667126983L;

	@Override
	public SampleResult sample(Entry entry) {
		SampleResult result = new SampleResult();
		result.sampleStart();
		JMeterVariables vars = JMeterContextService.getContext().getVariables();
		result.setSampleLabel(getName());
		SubSampler subSampler = (SubSampler) vars.getObject("sub");

		if (subSampler == null) {
			result.setSuccessful(false);
			result.setResponseMessage("Sampler must be preceded by an async sub sampler.");
			result.setResponseData("Failed. Sub sampler not found.".getBytes());
			result.setResponseCode("500");
			result.sampleEnd(); // avoid endtime=0 exposed in trace log
			return result;
		}

		return subSampler.produceAsyncResult(result);
	}
}
