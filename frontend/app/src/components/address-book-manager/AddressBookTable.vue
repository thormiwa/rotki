<script setup lang="ts">
import {
  NotificationCategory,
  type NotificationPayload,
  Severity
} from '@rotki/common/lib/messages';
import { type Ref } from 'vue';
import { type Blockchain } from '@rotki/common/lib/blockchain';
import { type DataTableHeader } from '@/types/vuetify';
import {
  type AddressBookEntries,
  type AddressBookEntry,
  type AddressBookLocation
} from '@/types/eth-names';

const props = withDefaults(defineProps<Props>(), {
  search: ''
});
const emit = defineEmits<{
  (e: 'edit', item: AddressBookEntry): void;
}>();
const { t } = useI18n();
const addressBookDeletion = (location: Ref<AddressBookLocation>) => {
  const { show } = useConfirmStore();
  const { notify } = useNotificationsStore();
  const { deleteAddressBook: deleteAddressBookCaller } =
    useAddressesNamesStore();
  const deleteAddressBook = async (
    address: string,
    blockchain: Blockchain | null
  ) => {
    try {
      await deleteAddressBookCaller(get(location), [{ address, blockchain }]);
    } catch (e: any) {
      const notification: NotificationPayload = {
        title: t('address_book.actions.delete.error.title'),
        message: t('address_book.actions.delete.error.description', {
          chain: blockchain || t('common.multi_chain'),
          address,
          message: e.message
        }).toString(),
        category: NotificationCategory.DEFAULT,
        display: true,
        severity: Severity.ERROR
      };
      notify(notification);
    }
  };

  const showDeleteConfirmation = (item: AddressBookEntry) => {
    show(
      {
        title: t('address_book.actions.delete.dialog.title'),
        message: t('address_book.actions.delete.dialog.message', {
          chain: item.blockchain || t('common.multi_chain'),
          address: item.address
        })
      },
      () => deleteAddressBook(item.address, item.blockchain)
    );
  };

  return {
    showDeleteConfirmation
  };
};

interface Props {
  location: AddressBookLocation;
  blockchain: Blockchain;
  search?: string;
}

const { location, search, blockchain } = toRefs(props);
const loading = ref<boolean>(false);

const addressesNamesStore = useAddressesNamesStore();
const { fetchAddressBook } = addressesNamesStore;
const { addressBookEntries } = toRefs(addressesNamesStore);

const data = computed<AddressBookEntries>(
  () => get(addressBookEntries)[get(location)]
);

const filteredData = computed<AddressBookEntries>(() => {
  const keyword = get(search).toLowerCase();
  const selectedChain = get(blockchain)?.toLowerCase();

  return get(data).filter(
    item =>
      (!keyword ||
        item.address.includes(keyword) ||
        item.name.toLowerCase().includes(keyword)) &&
      (item.blockchain === null ||
        item.blockchain?.toLowerCase() === selectedChain)
  );
});

const edit = (item: AddressBookEntry) => {
  emit('edit', item);
};

onBeforeMount(async () => {
  set(loading, true);
  await fetchAddressBook(get(location));
  set(loading, false);
});

const tableHeaders = computed<DataTableHeader[]>(() => [
  {
    text: t('common.address').toString(),
    value: 'address'
  },
  {
    text: t('common.name').toString(),
    value: 'name'
  },
  {
    text: '',
    value: 'actions',
    sortable: false
  }
]);

const { showDeleteConfirmation } = addressBookDeletion(location);
</script>

<template>
  <div>
    <DataTable :items="filteredData" :headers="tableHeaders" :loading="loading">
      <template #item.address="{ item }">
        <AccountDisplay
          :account="{
            address: item.address,
            chain: item.blockchain
          }"
          :use-alias-name="false"
          :truncate="false"
        />
      </template>
      <template #item.actions="{ item }">
        <RowActions
          :disabled="loading"
          :delete-tooltip="t('address_book.actions.delete.tooltip')"
          :edit-tooltip="t('address_book.actions.edit.tooltip')"
          @delete-click="showDeleteConfirmation(item)"
          @edit-click="edit(item)"
        />
      </template>
    </DataTable>
  </div>
</template>
