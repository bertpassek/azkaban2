package azkaban.webapp.servlet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;
import org.joda.time.DateTime;

import azkaban.executor.ExecutableFlow;
import azkaban.executor.ExecutorManagerAdapter;
import azkaban.executor.ExecutorManagerException;
import azkaban.executor.Status;
import azkaban.trigger.Condition;
import azkaban.trigger.Trigger;
import azkaban.trigger.TriggerAction;
import azkaban.trigger.TriggerManager;
import azkaban.trigger.builtin.ExecuteFlowAction;
import azkaban.utils.Utils;
import azkaban.webapp.AzkabanWebServer;

public class ZabbixServlet extends AbstractAzkabanServlet 
{
	private static final Logger LOGGER = Logger.getLogger(ZabbixServlet.class);
	private static final long serialVersionUID = -438345112105755601L;
	private static final String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";

	private TriggerManager triggerManager;
	private ExecutorManagerAdapter executorManager;
	
	@Override
	public void init(ServletConfig config) throws ServletException 
	{
		super.init(config);
		
		AzkabanWebServer server = (AzkabanWebServer) getApplication();
		executorManager = server.getExecutorManager();
		triggerManager = server.getTriggerManager();
	}

	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException 
	{
		List<ScheduledFlowResult> scheduledFlows = getScheduledFlowResults(triggerManager.getTriggers(), getParams(req));

		Page page = newPage(req, resp, null, "azkaban/webapp/servlet/velocity/zabbix.vm");
		page.add("scheduledFlows", scheduledFlows);
		
		boolean allSucceeded = true;
		for (ScheduledFlowResult flow : scheduledFlows)
		{
			allSucceeded &= flow.isOk;
		}
		page.add("allSucceeded", allSucceeded);
		
		page.render();
	}
	
	private MyParams getParams(final HttpServletRequest req)
	{
		MyParams params = new MyParams();
		
		String limitParam = req.getParameter("limit");
		if (limitParam != null)
		{
			params.limit = Integer.valueOf(limitParam);
		}

		String percentageFromAverage = req.getParameter("percentageFromAverage");
		if (percentageFromAverage != null)
		{
			params.percentageFromAverage = Double.valueOf(percentageFromAverage);
		}

		return params;
	}
	
	private List<ScheduledFlowResult> getScheduledFlowResults(final List<Trigger> triggers, final MyParams params) throws IOException
	{
		List<ScheduledFlowResult> results = new ArrayList<ScheduledFlowResult>();

		for (Trigger trigger : triggers)
		{
			ScheduledFlowResult flow = getScheduledFlowResult(trigger, params);
			if (flow != null)
			{
				results.add(flow);
			}
		}
		
		return results;
	}
	
	private ScheduledFlowResult getScheduledFlowResult(final Trigger trigger, final MyParams params) throws IOException
	{
		for (TriggerAction action : trigger.getActions())
		{
			if (action instanceof ExecuteFlowAction)
			{
				return getScheduledFlowResult(trigger, (ExecuteFlowAction) action, params);
			}
		}
		
		return null;
	}

	private ScheduledFlowResult getScheduledFlowResult(final Trigger trigger, final ExecuteFlowAction action, final MyParams params) throws IOException
	{
		LOGGER.debug("found trigger: " + trigger.toJson());
		try
		{
			List<ExecutableFlow> flows = new ArrayList<ExecutableFlow>();
			executorManager.getExecutableFlows(action.getProjectId(), action.getFlowName(), 0, 1, flows);
			LOGGER.info("got " + flows.size() + " flows for flow id '" + action.getFlowName() + "' and project id '" + action.getProjectId() + "'");
			
			ExecutableFlow flow = findFlow(flows);
			if (flow == null)
			{
				return null;
			}

			List<ExecutableFlow> succeededFlows = executorManager.getExecutableFlows(action.getProjectId(), action.getFlowName(), 0, params.limit, Status.SUCCEEDED);
			LOGGER.info("got " + succeededFlows.size() + " succeeded flows for flow id '" + action.getFlowName() + "' and project id '" + action.getProjectId() + "'");
			
			long lastSucceededTime = getLastRuntime(succeededFlows);
			long averageSucceededTime = getAverageRuntime(succeededFlows);
			long maxSucceededTime = getMaxRuntime(succeededFlows);
			
			ScheduledFlowResult scheduledFlow = new ScheduledFlowResult();
			scheduledFlow.flowName = action.getFlowName();
			scheduledFlow.startTime = flow.getStartTime() == -1 ? "" : new DateTime(flow.getStartTime()).toString(DATE_FORMAT);
			scheduledFlow.endTime = flow.getEndTime() == -1 ? "" : new DateTime(flow.getEndTime()).toString(DATE_FORMAT);
			scheduledFlow.lastSucceededRuntime = Utils.formatDuration(0, lastSucceededTime);
			scheduledFlow.averageSucceededRuntime = Utils.formatDuration(0, averageSucceededTime);
			scheduledFlow.maxSucceededRuntime = Utils.formatDuration(0, maxSucceededTime);

			Condition triggerCondition = trigger.getTriggerCondition();
			if (triggerCondition.getNextCheckTime() != -1)
			{
				scheduledFlow.nextExecutionTime = new DateTime(triggerCondition.getNextCheckTime()).toString(DATE_FORMAT);
			}
			else
			{
				scheduledFlow.nextExecutionTime = "undefined";
			}

			String status = flow.getStatus().toString().toLowerCase(Locale.getDefault());
			scheduledFlow.isOk = true;
			scheduledFlow.status = status;
			scheduledFlow.statusColor = "green";
			
			if (flow.getStatus().getNumVal() > Status.SUCCEEDED.getNumVal())
			{
				scheduledFlow.isOk = false;
				scheduledFlow.statusColor = "red";
			}
			else if (flow.getStatus().getNumVal() != Status.SUCCEEDED.getNumVal())
			{
				long maxRunningTimestampUntilFailure = flow.getStartTime() + maxSucceededTime + Math.round((double) averageSucceededTime * params.percentageFromAverage);
				LOGGER.info("flow " + flow.getFlowId() + " seems to be running, curren time is " + new DateTime(params.now).toString(DATE_FORMAT) + ", max running time is " + new DateTime(maxRunningTimestampUntilFailure).toString(DATE_FORMAT));
				
				if (maxRunningTimestampUntilFailure < params.now)
				{
					scheduledFlow.isOk = false;
					scheduledFlow.status += " (current runtime out of limit: " + new DateTime(maxRunningTimestampUntilFailure).toString(DATE_FORMAT) + ")";
					scheduledFlow.statusColor = "red";
				}
				else
				{
					scheduledFlow.isOk = true;
					scheduledFlow.status += " (current runtime in limit: " + new DateTime(maxRunningTimestampUntilFailure).toString(DATE_FORMAT) + ")";
				}
			}
			
			return scheduledFlow;
		} catch (ExecutorManagerException e) {
			throw new IOException(e);
		}
	}

	private ExecutableFlow findFlow(final List<ExecutableFlow> flows)
	{
		if (flows.isEmpty())
		{
			return null;
		}

		// we are only interested in the last flow which is at index 0 
		return flows.get(0);
	}

	private long getLastRuntime(final List<ExecutableFlow> flows)
	{
		for (ExecutableFlow flow : flows)
		{
			long start = flow.getStartTime();
			long end = flow.getEndTime();
			
			if (start != -1 && end != -1) 
			{
				return end - start;
			}
		}
		
		return 0;
	}

	private long getMaxRuntime(final List<ExecutableFlow> flows)
	{
		long runtime = 0L;
		
		for (ExecutableFlow flow : flows)
		{
			long start = flow.getStartTime();
			long end = flow.getEndTime();
			
			if (start != -1 && end != -1 && (end - start) > runtime) 
			{
				runtime = end - start;
			}
		}
		
		return runtime;
	}

	private long getAverageRuntime(final List<ExecutableFlow> flows)
	{
		long runtime = 0L;
		long finished = 0L;
		
		for (ExecutableFlow flow : flows)
		{
			long start = flow.getStartTime();
			long end = flow.getEndTime();
			
			if (start != -1 && end != -1) 
			{
				runtime += end - start;
				finished++;
			}
		}
		
		if (finished == 0)
		{
			return 0;
		}
		
		return Math.round((double) runtime / (double) finished);
	}
	
	public static class ScheduledFlowResult 
	{
		private String flowName = "";
		private String startTime = "";
		private String endTime = "";
		private String lastSucceededRuntime = "";
		private String averageSucceededRuntime = "";
		private String maxSucceededRuntime = "";
		private String nextExecutionTime = "";
		private String status = "";
		private String statusColor = "";
		private boolean isOk = true;
		
		public String getFlowName() 
		{
			return flowName;
		}
		
		public String getStatus() 
		{
			return status;
		}
		
		public String getStartTime() 
		{
			return startTime;
		}
		
		public String getEndTime() 
		{
			return endTime;
		}
		
		public String getLastSucceededRuntime() 
		{
			return lastSucceededRuntime;
		}
		
		public String getAverageSucceededRuntime() 
		{
			return averageSucceededRuntime;
		}
		
		public String getMaxSucceededRuntime() 
		{
			return maxSucceededRuntime;
		}
		
		public String getNextExecutionTime() 
		{
			return nextExecutionTime;
		}

		public String getStatusColor() 
		{
			return statusColor;
		}
	}
	
	private static class MyParams
	{
		private static final Integer DEFAULT_LIMIT = 30;
		private static final Double DEFAULT_PERCENTAGE_FROM_AVERAGE = 0.1;
		
		int limit = DEFAULT_LIMIT;
		double percentageFromAverage = DEFAULT_PERCENTAGE_FROM_AVERAGE;
		long now = System.currentTimeMillis(); 
	}
}