/*
 * Copyright (c) 2015-2017, Christoph Engelbert (aka noctarius) and
 * contributors. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.noctarius.snowcast.impl.operations.clientcodec;

import com.hazelcast.instance.MemberImpl;
import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.spi.EventRegistration;
import com.hazelcast.spi.EventService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;
import com.noctarius.snowcast.impl.NodeSequencerService;
import com.noctarius.snowcast.impl.SequencerDefinition;
import com.noctarius.snowcast.impl.notification.ClientDestroySequencerNotification;
import com.noctarius.snowcast.impl.operations.BackupDestroySequencerDefinitionOperation;
import com.noctarius.snowcast.impl.operations.DestroySequencerOperation;

import javax.annotation.Nonnull;
import java.util.Collection;

import static com.noctarius.snowcast.impl.SnowcastConstants.SERVICE_NAME;

class ClientDestroySequencerDefinitionOperation
        extends AbstractClientRequestOperation
        implements BackupAwareOperation {

    private transient int backupCount;

    ClientDestroySequencerDefinitionOperation(@Nonnull String sequencerName, @Nonnull MessageChannel messageChannel) {
        super(sequencerName, messageChannel);
    }

    @Override
    public void run()
            throws Exception {

        String sequencerName = getSequencerName();

        NodeSequencerService sequencerService = getService();
        SequencerDefinition definition = sequencerService.destroySequencer(sequencerName, true);

        // Definition might be already destroyed concurrently
        if (definition == null) {
            return;
        }

        backupCount = definition.getBackupCount();

        NodeEngine nodeEngine = getNodeEngine();

        OperationService operationService = nodeEngine.getOperationService();
        DestroySequencerOperation operation = new DestroySequencerOperation(sequencerName);
        for (MemberImpl member : nodeEngine.getClusterService().getMemberImpls()) {
            if (!member.localMember()) {
                operationService.invokeOnTarget(SERVICE_NAME, operation, member.getAddress());
            }
        }

        String clientUuid = getMessageChannel().getUuid();

        ClientDestroySequencerNotification notification = new ClientDestroySequencerNotification(sequencerName);
        Collection<EventRegistration> registrations = sequencerService.findClientChannelRegistrations(sequencerName, clientUuid);
        EventService eventService = nodeEngine.getEventService();
        eventService.publishEvent(SERVICE_NAME, registrations, notification, 1);
        eventService.deregisterAllListeners(SERVICE_NAME, sequencerName);
    }

    @Override
    public Object getResponse() {
        return Boolean.TRUE;
    }

    @Override
    public boolean shouldBackup() {
        return true;
    }

    @Override
    public int getSyncBackupCount() {
        return backupCount;
    }

    @Override
    public int getAsyncBackupCount() {
        return 0;
    }

    @Override
    public Operation getBackupOperation() {
        return new BackupDestroySequencerDefinitionOperation(getSequencerName());
    }
}
