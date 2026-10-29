import type React from 'react';

interface FormRowProps {
  label: string;
  /** The control the label names, for a single field. */
  htmlFor?: string;
  /** The id a group points its aria-labelledby at, for a set of controls. */
  labelId?: string;
  children: React.ReactNode;
}

/**
 * One setting: its name in the left column, the field on the right, a
 * hairline between rows. The column is the one the starting points list
 * draws its cadence in, so the form lines up under what it was opened from.
 */
export default function FormRow({ label, htmlFor, labelId, children }: FormRowProps) {
  return (
    <div className="automation-form-row">
      {htmlFor ? (
        <label className="automation-form-label" htmlFor={htmlFor}>
          {label}
        </label>
      ) : (
        <span className="automation-form-label" id={labelId}>
          {label}
        </span>
      )}
      <div className="automation-form-field">{children}</div>
    </div>
  );
}
