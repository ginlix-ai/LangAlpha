import React, { useState } from 'react';
import { Eye, EyeOff } from 'lucide-react';
import { useTranslation } from 'react-i18next';
import { Input } from '../../components/ui/input';

type PasswordInputProps = Omit<React.InputHTMLAttributes<HTMLInputElement>, 'type'>;

/** Password field with a show/hide toggle. Styling comes from LoginPage.css. */
function PasswordInput(props: PasswordInputProps) {
  const [visible, setVisible] = useState(false);
  const { t } = useTranslation();

  return (
    <div className="login-page__password-wrap">
      <Input type={visible ? 'text' : 'password'} {...props} />
      <button
        type="button"
        className="login-page__password-toggle"
        onClick={() => setVisible((v) => !v)}
        aria-label={visible ? t('auth.hidePassword') : t('auth.showPassword')}
      >
        {visible ? <EyeOff size={16} /> : <Eye size={16} />}
      </button>
    </div>
  );
}

export default PasswordInput;
