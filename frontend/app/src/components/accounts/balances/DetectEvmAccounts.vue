<script setup lang="ts">
import { TaskType } from '@/types/task-type';

const { isTaskRunning } = useTaskStore();
const isEvmAccountsDetecting = isTaskRunning(TaskType.DETECT_EVM_ACCOUNTS);
const { detectEvmAccounts } = useBlockchains();

const { t } = useI18n();
</script>

<template>
  <VTooltip right max-width="300">
    <template #activator="{ on }">
      <VBtn
        color="primary"
        depressed
        :loading="isEvmAccountsDetecting"
        :disabled="isEvmAccountsDetecting"
        v-on="on"
        @click="detectEvmAccounts()"
      >
        {{ t('blockchain_balances.evm_detection.title') }}
      </VBtn>
    </template>
    <span>
      {{ t('blockchain_balances.evm_detection.tooltip') }}
    </span>
  </VTooltip>
</template>
