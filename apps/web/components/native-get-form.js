export function NativeGetForm({ action = "", className, children }) {
  return (
    <form action={action} method="get" className={className}>
      {children}
    </form>
  );
}
