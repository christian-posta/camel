/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.camel.component.aws.sqs;

import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.amazonaws.services.sqs.model.Message;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.builder.NotifyBuilder;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.impl.JndiRegistry;
import org.apache.camel.test.junit4.CamelTestSupport;
import org.junit.Test;



/**
 * Created by ceposta
 * <a href="http://christianposta.com/blog>http://christianposta.com/blog</a>.
 */
public class SqsConcurrentConsumerTest extends CamelTestSupport {
    private static final int NUM_CONCURRENT = 10;
    private static final int NUM_MESSAGES = 100;

    private final short[] threadIndexes = new short[NUM_CONCURRENT];

    @Test
    public void consumeMessagesFromQueue() throws Exception {
        NotifyBuilder notifier = new NotifyBuilder(context).whenCompleted(NUM_MESSAGES).create();
        assertTrue("We didn't process "
                + NUM_MESSAGES
                + " messages as we expected!", notifier.matches(5, TimeUnit.SECONDS));

        // if all 10 threads were used in the concurrentConsumer test, we would have
        // a n > 0 for each index in the array:
        for (int i = 0; i < NUM_CONCURRENT; i++) {
            assertTrue("We did not get a thread for " + i + " when testing concurrent consumers",
                    threadIndexes[i] > 0);
        }

    }

    @Override
    protected JndiRegistry createRegistry() throws Exception {
        JndiRegistry reg = super.createRegistry();
        AmazonSQSClientMock client = new AmazonSQSClientMock();
        createDummyMessages(client, NUM_MESSAGES);
        reg.bind("client", client);
        return reg;
    }

    private void createDummyMessages(AmazonSQSClientMock client, int numMessages) {
        for (int counter = 0; counter < numMessages; counter++) {
            Message message = new Message();
            message.setBody("Message " + counter);
            message.setMD5OfBody("6a1559560f67c5e7a7d5d838bf0272ee");
            message.setMessageId("f6fb6f99-5eb2-4be4-9b15-144774141458");
            message.setReceiptHandle("0NNAq8PwvXsyZkR6yu4nQ07FGxNmOBWi5");
            client.messages.add(message);
        }
    }


    @Override
    protected RouteBuilder createRouteBuilder() throws Exception {
        return new RouteBuilder() {
            @Override
            public void configure() throws Exception {
                from("aws-sqs://demo?concurrentConsumers=" + NUM_CONCURRENT + "&maxMessagesPerPoll=10&amazonSQSClient=#client")
                        .process(new Processor() {
                            @Override
                            public void process(Exchange exchange) throws Exception {
                                int threadNumber = extractThreadNumber(Thread.currentThread().getName());
                                threadIndexes[threadNumber]++;
                            }
                        }).log("processed a new message!");
            }
        };
    }

    int extractThreadNumber(String threadName) {
        Pattern threadNumberPattern = Pattern.compile("[^#]+[#]([1-9]).*");
        Matcher matcher = threadNumberPattern.matcher(threadName);

        int num = 0;
        try {
            if (matcher.find()) {
                String numStr = matcher.group(1);
                num = Integer.parseInt(numStr);
            }
        } catch (NumberFormatException e) {
            num = -1;
        }

        return num;
    }

}
