package learn.jyOverQ.transaction;

import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

// 内存 Map 仅模拟本地事务，真实业务必须查询持久化状态。
public class DemoTransactionListener implements TransactionListener {

    private final Map<String, LocalTransactionState> txStatus = new ConcurrentHashMap<>();

    @Override
    public LocalTransactionState executeLocalTransaction(Message message, Object arg) {
        String orderId = message.getUserProperty("orderId");
        String scenario = String.valueOf(arg);

        if ("rollback".equals(scenario)) {
            txStatus.put(orderId, LocalTransactionState.ROLLBACK_MESSAGE);
            System.out.println("本地事务失败，准备回滚消息，orderId=" + orderId);
            return LocalTransactionState.ROLLBACK_MESSAGE;
        }

        if ("unknown".equals(scenario)) {
            // 模拟本地事务已提交，但第一次无法向 Broker 确认结果。
            txStatus.put(orderId, LocalTransactionState.COMMIT_MESSAGE);
            System.out.println("本地事务结果暂时无法确认，orderId=" + orderId);
            return LocalTransactionState.UNKNOW;
        }

        txStatus.put(orderId, LocalTransactionState.COMMIT_MESSAGE);
        System.out.println("本地事务执行成功：" + orderId);
        return LocalTransactionState.COMMIT_MESSAGE;
    }

    @Override
    public LocalTransactionState checkLocalTransaction(MessageExt message) {
        String orderId = message.getUserProperty("orderId");
        LocalTransactionState state = orderId == null ? null : txStatus.get(orderId);
        LocalTransactionState result = state == null ? LocalTransactionState.UNKNOW : state;
        System.out.println("事务回查，orderId=" + orderId + "，结果=" + result);
        return result;
    }
}
