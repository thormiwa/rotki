<script setup lang="ts">
import { supportedLanguages } from '@/data/supported-language';
import { SupportedLanguage } from '@/types/frontend-settings';

const props = defineProps({
  dense: {
    required: false,
    type: Boolean,
    default: false
  },
  showLabel: {
    required: false,
    type: Boolean,
    default: true
  },
  useLocalSetting: {
    required: false,
    type: Boolean,
    default: false
  }
});

const { useLocalSetting } = toRefs(props);

const language = ref<string>(SupportedLanguage.EN);

const { lastLanguage } = useLastLanguage();

const updateSetting = async (
  value: string,
  update: (newValue: any) => Promise<void>
) => {
  if (get(useLocalSetting)) {
    set(lastLanguage, value);
  } else {
    await update(value);
  }
};

const { forceUpdateMachineLanguage } = useLastLanguage();
const { adaptiveLanguage } = storeToRefs(useSessionStore());

const updateForceUpdateMachineLanguage = (event: boolean | null) => {
  set(forceUpdateMachineLanguage, event ? 'true' : 'false');
};

onMounted(() => {
  set(language, get(adaptiveLanguage));
});

const { t } = useI18n();
const rootAttrs = useAttrs();

const { languageContributeUrl } = useInterop();
</script>

<template>
  <div>
    <div class="d-flex align-center">
      <SettingsOption
        #default="{ error, success, update }"
        class="w-full"
        setting="language"
        frontend-setting
        :error-message="t('general_settings.validation.language.error')"
      >
        <VSelect
          v-model="language"
          :items="supportedLanguages"
          item-text="label"
          item-value="identifier"
          outlined
          hide-details
          :label="t('general_settings.labels.language')"
          persistent-hint
          :success-messages="success"
          :error-messages="error"
          v-bind="rootAttrs"
          @change="updateSetting($event, update)"
        >
          <template #item="{ item }">
            <LanguageSelectorItem
              :countries="item.countries ?? [item.identifier]"
              :label="item.label"
            />
          </template>
          <template #selection="{ item }">
            <LanguageSelectorItem
              :countries="item.countries ?? [item.identifier]"
              :label="item.label"
            />
          </template>
        </VSelect>
      </SettingsOption>
      <div class="ml-2">
        <VTooltip open-delay="400" bottom max-width="400">
          <template #activator="{ on }">
            <BaseExternalLink :href="languageContributeUrl">
              <VBtn icon v-on="on">
                <VIcon>mdi-account-edit</VIcon>
              </VBtn>
            </BaseExternalLink>
          </template>
          <span>
            {{ t('general_settings.language_contribution_tooltip') }}
          </span>
        </VTooltip>
      </div>
    </div>
    <div v-if="!useLocalSetting" class="mb-n10">
      <VRow>
        <VCol cols="auto">
          <VCheckbox
            :input-value="forceUpdateMachineLanguage === 'true'"
            :label="
              t(
                'general_settings.labels.force_saved_language_setting_in_machine_hint'
              )
            "
            @change="updateForceUpdateMachineLanguage($event)"
          />
        </VCol>
      </VRow>
    </div>
  </div>
</template>
