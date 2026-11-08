import * as React from "react"
import { buttonClass, buttonSlot, type ButtonSize, type ButtonTone, type ButtonVariant } from "./button-class"

export interface ButtonProps extends React.ComponentProps<"button"> {
  variant?: ButtonVariant
  size?: ButtonSize
  tone?: ButtonTone
}

function Button({ className, variant, size, tone, ...props }: ButtonProps) {
  return (
    <button
      {...buttonSlot(variant, tone)}
      className={buttonClass({ variant, size, className })}
      {...props}
    />
  )
}

export { Button }
