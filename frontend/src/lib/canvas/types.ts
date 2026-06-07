export type PaneType = 'room' | 'directory' | 'profile' | 'admin' | 'roominfo';

export interface PaneState {
  id: string;       // room_name, '__directory__', '__profile__', '__admin__', or 'roominfo:<room>'
  title: string;    // display label in title bar
  type: PaneType;
  x: number;
  y: number;
  width: number;
  height: number;
  z: number;        // stacking order; higher = on top
  readOnly?: boolean; // room panes opened by an admin for moderation (no composer)
  unread?: number;
}
