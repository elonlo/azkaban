package azkaban.utils;

import azkaban.alert.Alerter;
import azkaban.executor.ExecutableFlow;
import azkaban.executor.Executor;
import azkaban.executor.ExecutorManagerException;
import azkaban.sla.SlaOption;
import org.apache.commons.lang.StringUtils;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.log4j.Logger;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.text.SimpleDateFormat;
import java.util.List;

@Singleton
public class SMSAlerter implements Alerter {
  private static final Logger logger = Logger.getLogger(SMSAlerter.class);
  private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");

  private CloseableHttpClient httpClient = HttpClients.createDefault();
  private final String azkabanName;
  private final String url;
  private final String account;
  private final String password;
  private boolean testMode = false;

  @Inject
  public SMSAlerter(Props props) {
    this.azkabanName = props.getString("azkaban.name", "azkaban");
    this.url = props.getString("sms.url", "");
    this.account = props.getString("sms.account", "");
    this.password = props.getString("sms.password", "");
    this.testMode = props.getBoolean("sms.test.model", false);
  }

  @Override
  public void alertOnSuccess(ExecutableFlow exflow) throws Exception {
    sendSMS(exflow.getExecutionOptions().getSuccessPhones(), getMessage(exflow, "Success"));
  }

  @Override
  public void alertOnError(ExecutableFlow exflow, String... extraReasons) throws Exception {
    sendSMS(exflow.getExecutionOptions().getFailurePhones(), getMessage(exflow, "Failure"));
  }

  @Override
  public void alertOnFirstError(ExecutableFlow exflow) throws Exception {
    sendSMS(exflow.getExecutionOptions().getFailurePhones(), getMessage(exflow, "First Failure"));
  }

  @Override
  public void alertOnSla(SlaOption slaOption, String slaMessage) throws Exception {
    logger.info("SLA告警短信通知为实现，发送忽略");
  }

  @Override
  public void alertOnFailedUpdate(Executor executor, List<ExecutableFlow> executions, ExecutorManagerException e) {

  }

  /**
   * 获取消息内容
   *
   * @param flow
   * @param status
   * @return
   */
  private String getMessage(ExecutableFlow flow, String status) throws UnsupportedEncodingException {
    String msg = String.format("【Azkaban】%s任务执行%s，EID: %d，开始时间: %s，运行时长: %s",
      flow.getFlowId(), status, flow.getExecutionId(), dateFormat.format(flow.getStartTime()),
      Utils.formatDuration(flow.getStartTime(), flow.getEndTime()));
    return URLEncoder.encode(msg, "gb2312");
  }

  /**
   * 发送短信
   *
   * @param phones
   * @param msg
   */
  private void sendSMS(List<String> phones, String msg) {
    if (StringUtils.isEmpty(msg) || phones.isEmpty()) {
      logger.warn("SMS发送内容或接收人为空，跳过发送");
      return;
    }
    for (String phone : phones) {
      String getUrl = String.format(url, account, password, phone, msg);
      HttpGet httpGet = new HttpGet(getUrl);
      CloseableHttpResponse response = null;
      logger.info(String.format("开始发送短信：%s url: %s", msg, getUrl));
      try {
        if (!testMode) {
          response = httpClient.execute(httpGet);
          logger.info(String.format("当前发送至：%s，发送状态为：%s", phone, response.getStatusLine()));
        } else {
          logger.info(String.format("当前为测试模式,欲发送至：%s", phone));
        }
      } catch (ClientProtocolException e) {
        e.printStackTrace();
      } catch (IOException e) {
        e.printStackTrace();
      } finally {
        if (response != null) {
          try {
            response.close();
          } catch (IOException e) {
            e.printStackTrace();
          }
        }
      }
    }
  }
}
