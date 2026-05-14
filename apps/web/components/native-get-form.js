export function NativeGetForm({ action = "", className, children, ...props }) {
  return (
    <form action={action} method="get" className={className} {...props}>
      {children}
    </form>
  );
}
