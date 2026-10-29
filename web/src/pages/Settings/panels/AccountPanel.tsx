import React, { useCallback, useEffect, useId, useRef, useState } from 'react';
import { User, LogOut } from 'lucide-react';
import { useQueryClient } from '@tanstack/react-query';
import { useTranslation } from 'react-i18next';
import { Input } from '@/components/ui/input';
import { Button } from '@/components/ui/button';
import { updateCurrentUser, uploadAvatar } from '@/pages/Dashboard/utils/api';
import { useAuth } from '@/contexts/AuthContext';
import { useUser } from '@/hooks/useUser';
import { queryKeys } from '@/lib/queryKeys';
import ConfirmDialog from '@/pages/Dashboard/components/ConfirmDialog';
import { useDebouncedSave } from '@/hooks/useDebouncedSave';
import { isImeComposing } from '@/lib/ime';
import { SettingRow, SettingsGroup } from '../components/SettingsLayout';
import { useSettingCopy } from '../components/useSettingCopy';
import { SETTINGS } from '../sections';
import { SaveStatus } from '../components/SaveStatus';

/** Profile: photo, name and email, and signing out. */
export function AccountPanel() {
  const { logout } = useAuth();
  const { user: authUser } = useUser();
  const queryClient = useQueryClient();
  const { t } = useTranslation();
  const copy = useSettingCopy();
  const nameId = useId();

  // What this panel changed, over what the account says until it says so too.
  const [uploadedAvatar, setUploadedAvatar] = useState<string | null>(null);
  const [brokenAvatar, setBrokenAvatar] = useState<string | null>(null);
  const [nameDraft, setNameDraft] = useState<string | null>(null);
  const fileInputRef = useRef<HTMLInputElement>(null);
  const [isUploadingAvatar, setIsUploadingAvatar] = useState(false);
  const [avatarError, setAvatarError] = useState<string | null>(null);
  const [showLogoutConfirm, setShowLogoutConfirm] = useState(false);

  const storedAvatar = authUser?.avatar_url ? `${authUser.avatar_url}?v=${authUser.updated_at || ''}` : null;
  const shownAvatar = uploadedAvatar ?? storedAvatar;
  const avatarUrl = shownAvatar === brokenAvatar ? null : shownAvatar;
  const name = nameDraft ?? authUser?.name ?? '';

  const handleAvatarChange = async (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    if (!file) return;
    setIsUploadingAvatar(true);
    setAvatarError(null);
    try {
      const { avatar_url } = await uploadAvatar(file) as { avatar_url: string };
      setUploadedAvatar(`${avatar_url}?t=${Date.now()}`);
      queryClient.invalidateQueries({ queryKey: queryKeys.user.me() });
    } catch {
      setAvatarError(t('settings.failedToUploadAvatar'));
    } finally {
      setIsUploadingAvatar(false);
    }
  };

  // The debounced save reads the latest name through a ref.
  const nameRef = useRef('');
  const dirtyRef = useRef(false);

  const saveName = useCallback(async () => {
    dirtyRef.current = false;
    const trimmed = nameRef.current.trim();
    if (!trimmed) return false;
    await updateCurrentUser({ name: trimmed });
    queryClient.invalidateQueries({ queryKey: queryKeys.user.me() });
  }, [queryClient]);

  const { trigger: triggerNameSave, flush: flushNameSave, status: nameSaveStatus } = useDebouncedSave(saveName, 800);

  // This panel unmounts on a section switch, and useDebouncedSave cancels its
  // timer on unmount: flush a pending edit so it isn't silently lost.
  useEffect(() => () => { if (dirtyRef.current) flushNameSave(); }, [flushNameSave]);

  return (
    <>
      <SettingsGroup title={t('settings.profileGroup')} action={<SaveStatus status={nameSaveStatus} />}>
        <SettingRow
          {...copy(SETTINGS.photo)}
          description={avatarError
            ? <span style={{ color: 'var(--color-loss)' }}>{avatarError}</span>
            : t(SETTINGS.photo.descKey)}
        >
          <div className="flex items-center gap-3">
            <button
              type="button"
              className="h-12 w-12 rounded-xl flex items-center justify-center overflow-hidden shrink-0"
              style={{ backgroundColor: 'var(--color-accent-soft)' }}
              onClick={() => fileInputRef.current?.click()}
              aria-label={t('settings.changeAvatar')}
            >
              {avatarUrl ? (
                <img src={avatarUrl} alt="" className="h-full w-full object-cover" onError={() => setBrokenAvatar(avatarUrl)} />
              ) : (
                <User className="h-6 w-6" style={{ color: 'var(--color-accent-primary)' }} />
              )}
            </button>
            <Button
              type="button"
              variant="outline"
              size="sm"
              className="h-8"
              onClick={() => fileInputRef.current?.click()}
              disabled={isUploadingAvatar}
            >
              {isUploadingAvatar ? t('settings.uploading') : t('settings.changeAvatar')}
            </Button>
          </div>
          <input
            type="file"
            ref={fileInputRef}
            onChange={handleAvatarChange}
            accept="image/png,image/jpeg,image/gif,image/webp"
            className="hidden"
          />
        </SettingRow>
        <SettingRow {...copy(SETTINGS.name)} htmlFor={nameId} wide>
          <Input
            id={nameId}
            type="text"
            value={name}
            onChange={(e) => {
              setNameDraft(e.target.value);
              nameRef.current = e.target.value;
              dirtyRef.current = true;
              triggerNameSave();
            }}
            onBlur={() => {
              // A name cannot be blank: an emptied field goes back to the
              // saved one rather than looking saved while nothing was.
              if (!nameRef.current.trim()) {
                dirtyRef.current = false;
                setNameDraft(null);
                return;
              }
              flushNameSave();
            }}
            onKeyDown={(e) => { if (e.key === 'Enter' && !isImeComposing(e)) e.currentTarget.blur(); }}
            placeholder={t('auth.enterName')}
            className="h-9"
          />
        </SettingRow>
        <SettingRow {...copy(SETTINGS.email)}>
          <span className="text-[0.8125rem] break-all" style={{ color: 'var(--color-text-secondary)' }}>
            {authUser?.email || ''}
          </span>
        </SettingRow>
      </SettingsGroup>

      <SettingsGroup>
        <SettingRow {...copy(SETTINGS.logout)}>
          <Button
            type="button"
            variant="outline"
            size="sm"
            className="h-8 gap-1.5"
            style={{ color: 'var(--color-loss)' }}
            onClick={() => setShowLogoutConfirm(true)}
          >
            <LogOut className="h-3.5 w-3.5" /> {t('settings.logout')}
          </Button>
        </SettingRow>
      </SettingsGroup>

      <ConfirmDialog
        open={showLogoutConfirm}
        title={t('settings.logout')}
        message={t('settings.logoutConfirmMsg')}
        confirmLabel={t('settings.logout')}
        onConfirm={() => {
          logout();
          setShowLogoutConfirm(false);
        }}
        onOpenChange={setShowLogoutConfirm}
      />
    </>
  );
}
