export const SETTING_DISTRIBUTION_SHOW = "show";
export const SETTING_DISTRIBUTION_HIDE = "hide";

export function normalizeSettingDistribution(value) {
  return value === SETTING_DISTRIBUTION_SHOW ? SETTING_DISTRIBUTION_SHOW : SETTING_DISTRIBUTION_HIDE;
}

export function shouldShowSettingDistribution(value) {
  return normalizeSettingDistribution(value) === SETTING_DISTRIBUTION_SHOW;
}
