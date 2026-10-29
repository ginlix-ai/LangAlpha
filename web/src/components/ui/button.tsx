import * as React from "react"
import { buttonClass, type ButtonSize, type ButtonVariant } from "./button-class"

export interface ButtonProps extends React.ComponentProps<"button"> {
  variant?: ButtonVariant
  size?: ButtonSize
}

function Button({ className, variant, size, ...props }: ButtonProps) {
  return <button className={buttonClass({ variant, size, className })} {...props} />
}

export { Button }
